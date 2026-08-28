package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
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
	rpcTimeout     time.Duration
	version        bool
	appVersion     float32 = 0.1

	fetchMu sync.Mutex

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
	flag.StringVar(&username, "username", "", "Username for Supervisord authentication (prefer SUPERVISORD_USERNAME env var)")
	flag.StringVar(&password, "password", "", "Password for Supervisord authentication (prefer SUPERVISORD_PASSWORD env var to avoid leaking it via process listings)")
	flag.DurationVar(&rpcTimeout, "supervisord-timeout", 10*time.Second, "Timeout for XML-RPC requests to Supervisord")
	flag.BoolVar(&version, "version", false, "Displays application version")

	prometheus.MustRegister(processesMetric)
	prometheus.MustRegister(supervisorProcessUptime)
	prometheus.MustRegister(supervisordUp)
}

// timeoutTransport bounds the total time spent reading a request's headers and body,
// canceling the request context when the response body is closed.
type timeoutTransport struct {
	Transport http.RoundTripper
	Timeout   time.Duration
}

func (t *timeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), t.Timeout)
	resp, err := t.Transport.RoundTrip(req.WithContext(ctx))
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

// cancelOnCloseBody cancels its associated context once the response body is closed,
// which is when the xmlrpc codec is done reading it.
type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// createTransport creates an HTTP transport that supports both HTTP and Unix socket connections.
// It also returns the base *http.Transport so callers can release its idle connections themselves:
// once wrapped in timeoutTransport (and possibly authenticatedTransport), kolo/xmlrpc's own
// best-effort CloseIdleConnections() type-assertion on Client.Close() no longer matches.
func createTransport(targetURL string) (http.RoundTripper, string, *http.Transport, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, "", nil, fmt.Errorf("failed to parse URL: %v", err)
	}

	var baseTransport *http.Transport
	clientURL := targetURL

	if parsedURL.Scheme == "unix" {
		// A unix:// URL must use three slashes (unix:///path/to.sock) so the whole
		// path lands in parsedURL.Path; with only two slashes, url.Parse silently
		// puts the first path segment into Host instead, which would dial the
		// wrong socket without any error.
		if parsedURL.Host != "" {
			return nil, "", nil, fmt.Errorf("invalid unix socket URL %q: use unix:///path/to/socket (three slashes)", targetURL)
		}
		// For Unix sockets, we need to use a custom transport
		socketPath := parsedURL.Path
		baseTransport = &http.Transport{
			DialContext: func(ctx context.Context, proto, addr string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			},
		}
		// Return a fake HTTP URL for the xmlrpc client, the transport will handle the actual connection
		clientURL = "http://localhost/RPC2"
	} else {
		// For HTTP/HTTPS, use a default transport clone
		baseTransport = http.DefaultTransport.(*http.Transport).Clone()
	}

	var transport http.RoundTripper = baseTransport

	// Apply authentication if credentials are provided
	if username != "" && password != "" {
		transport = &authenticatedTransport{
			Transport: transport,
			Username:  username,
			Password:  password,
		}
	}

	// Bound the whole request/response cycle so a hung or slow Supervisord
	// can't block a scrape indefinitely.
	return &timeoutTransport{Transport: transport, Timeout: rpcTimeout}, clientURL, baseTransport, nil
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

// processKey uniquely identifies a process by its name and group. Using a
// struct instead of concatenating the strings avoids collisions between
// distinct (name, group) pairs (e.g. ("proc1", "A") vs ("proc", "1A")).
type processKey struct {
	name  string
	group string
}

func fetchSupervisorProcessInfo() {
	// Serialize scrapes: Reset() followed by repopulation is not atomic,
	// so overlapping scrapes could otherwise observe empty/partial metrics.
	fetchMu.Lock()
	defer fetchMu.Unlock()

	transport, clientURL, baseTransport, err := createTransport(supervisordURL)
	if err != nil {
		log.Printf("Error creating transport: %v", err)
		supervisordUp.Set(0)
		processesMetric.Reset()
		supervisorProcessUptime.Reset()
		return
	}
	defer baseTransport.CloseIdleConnections()

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
	latestInfo := make(map[processKey]map[string]interface{})

	for _, data := range result {
		name, _ := data["name"].(string)
		group, _ := data["group"].(string)

		key := processKey{name: name, group: group}

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
		// kolo/xmlrpc decodes XML-RPC <int> values into interface{} as int64, not int.
		exitStatus, _ := data["exitstatus"].(int64)
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

var promHandler = promhttp.Handler()

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	fetchSupervisorProcessInfo()
	promHandler.ServeHTTP(w, r)
}

func main() {
	flag.Parse()

	// Prefer environment variables for credentials over CLI flags, which are
	// visible to other local users via the process list (e.g. `ps aux`).
	if v := os.Getenv("SUPERVISORD_USERNAME"); v != "" {
		username = v
	}
	if v := os.Getenv("SUPERVISORD_PASSWORD"); v != "" {
		password = v
	}
	if (username != "") != (password != "") {
		log.Printf("Warning: only one of username/password is set; Supervisord authentication will be disabled")
	}

	if version {
		fmt.Printf("Supervisor Exporter v%v\n", appVersion)
		os.Exit(0)
	}

	http.HandleFunc(metricsPath, metricsHandler)

	server := &http.Server{
		Addr:              listenAddress,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      rpcTimeout + 10*time.Second,
	}

	fmt.Printf("Listening on %s\n", listenAddress)
	if err := server.ListenAndServe(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}
