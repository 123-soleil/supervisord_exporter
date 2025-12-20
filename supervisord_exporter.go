package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/kolo/xmlrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	supervisordURL string
	listenAddress  string
	metricsPath    string
	username       string
	password       string
	version        bool
	appVersion     float32 = 0.1

	processesMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "supervisor_process_info",
			Help: "Supervisor process information",
		},
		[]string{"name", "group", "state", "exit_status"},
	)
	supervisorProcessUptime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "supervisor_process_uptime",
			Help: "Uptime of Supervisor processes",
		},
		[]string{"name", "group"},
	)
	supervisordUp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "supervisord_up",
			Help: "Supervisord XML-RPC connection status (1 if up, 0 if down)",
		},
	)
)

func init() {
	flag.StringVar(&supervisordURL, "supervisord-url", "http://localhost:9001/RPC2", "Supervisord XML-RPC URL (supports http:// and unix:// schemes)")
	flag.StringVar(&listenAddress, "web.listen-address", ":9876", "Address to listen for HTTP requests")
	flag.StringVar(&metricsPath, "web.telemetry-path", "/metrics", "Path under which to expose metrics")
	flag.StringVar(&username, "username", "", "Username for Supervisord authentication")
	flag.StringVar(&password, "password", "", "Password for Supervisord authentication")
	flag.BoolVar(&version, "version", false, "Displays application version")

	flag.Parse()

	prometheus.MustRegister(processesMetric)
	prometheus.MustRegister(supervisorProcessUptime)
	prometheus.MustRegister(supervisordUp)
}

// createTransport creates an HTTP transport that supports both HTTP and Unix socket connections
func createTransport(targetURL string) (http.RoundTripper, string, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse URL: %v", err)
	}

	if parsedURL.Scheme == "unix" {
		// For Unix sockets, we need to use a custom transport
		socketPath := parsedURL.Path
		transport := &http.Transport{
			DialContext: func(ctx context.Context, proto, addr string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			},
		}
		
		// Apply authentication if credentials are provided
		if username != "" && password != "" {
			return &authenticatedTransport{
				Transport: transport,
				Username:  username,
				Password:  password,
			}, "http://localhost/RPC2", nil
		}
		
		// Return a fake HTTP URL for the xmlrpc client, the transport will handle the actual connection
		return transport, "http://localhost/RPC2", nil
	}

	// For HTTP/HTTPS, use default transport with authentication if provided
	if username != "" && password != "" {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		return &authenticatedTransport{
			Transport: transport,
			Username:  username,
			Password:  password,
		}, targetURL, nil
	}

	return nil, targetURL, nil
}

// authenticatedTransport wraps http.RoundTripper to add Basic Authentication
type authenticatedTransport struct {
	Transport http.RoundTripper
	Username  string
	Password  string
}

func (t *authenticatedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	reqCopy := req.Clone(req.Context())
	reqCopy.SetBasicAuth(t.Username, t.Password)
	return t.Transport.RoundTrip(reqCopy)
}

func fetchSupervisorProcessInfo() {
	transport, clientURL, err := createTransport(supervisordURL)
	if err != nil {
		log.Printf("Error creating transport: %v", err)
		supervisordUp.Set(0)
		processesMetric.Reset()
		supervisorProcessUptime.Reset()
		return
	}

	client, err := xmlrpc.NewClient(clientURL, transport)
	if err != nil {
		log.Printf("Error creating Supervisor XML-RPC client: %v", err)
		supervisordUp.Set(0)
		processesMetric.Reset()
		supervisorProcessUptime.Reset()
		return
	}
	defer client.Close()

	result := []map[string]interface{}{}
	if err := client.Call("supervisor.getAllProcessInfo", nil, &result); err != nil {
		log.Printf("Error calling Supervisor XML-RPC method: %v", err)
		supervisordUp.Set(0)
		processesMetric.Reset()
		supervisorProcessUptime.Reset()
		return
	}

	supervisordUp.Set(1)

	// Create a map to store the latest process information for each unique combination of name and group
	latestInfo := make(map[string]map[string]interface{})

	for _, data := range result {
		name, _ := data["name"].(string)
		group, _ := data["group"].(string)

		// Generate a unique key for the combination of name and group
		key := name + group

		// Check if the latest information for this combination already exists
		if existing, ok := latestInfo[key]; ok {
			// Compare timestamps to determine which information is more recent
			existingStartTime, _ := existing["start"].(int64)
			newStartTime, _ := data["start"].(int64)

			// If the new information is more recent, update the latestInfo map
			if newStartTime > existingStartTime {
				latestInfo[key] = data
			}
		} else {
			// If no previous information exists for this combination, add it to the map
			latestInfo[key] = data
		}
	}

	// Clear the previous metric values
	processesMetric.Reset()
	supervisorProcessUptime.Reset()

	for _, data := range latestInfo {
		name, _ := data["name"].(string)
		group, _ := data["group"].(string)
		state, _ := data["statename"].(string)
		exitStatus, _ := data["exitstatus"].(int)
		startTime, _ := data["start"].(int64)

		value := 0
		if state == "RUNNING" {
			value = 1
		}

		processesMetric.WithLabelValues(name, group, state, fmt.Sprintf("%d", exitStatus)).Set(float64(value))

		// Calculate uptime and set the supervisor_process_uptime metric
		if value == 1 {
			uptime := time.Now().Unix() - startTime
			supervisorProcessUptime.WithLabelValues(name, group).Set(float64(uptime))
		}
	}
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	fetchSupervisorProcessInfo()
	promhttp.Handler().ServeHTTP(w, r)
}

func main() {
	if version {
		fmt.Printf("Supervisor Exporter v%v\n", appVersion)
		os.Exit(0)
	}

	http.HandleFunc(metricsPath, metricsHandler)

	fmt.Printf("Listening on %s\n", listenAddress)
	if err := http.ListenAndServe(listenAddress, nil); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}
