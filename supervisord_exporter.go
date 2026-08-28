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
	"strconv"
	"sync"
	"time"

	"github.com/kolo/xmlrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"
)

var (
	supervisordURL string
	listenAddress  string
	metricsPath    string
	username       string
	password       string
	rpcTimeout     time.Duration
	version        bool
	appVersion     = "0.1"

	// rpcTransport and rpcClient are built once at startup from the (static)
	// -supervisord-url/-username/-password/-supervisord-timeout flags and reused
	// across scrapes: this avoids paying a fresh connection handshake and
	// xmlrpc.Client setup on every scrape. rpc.Client (which xmlrpc.Client wraps)
	// is safe to reuse for repeated sequential calls.
	rpcTransport http.RoundTripper
	rpcClientURL string
	rpcClient    *xmlrpc.Client

	// fetchGroup coalesces concurrent scrapes into a single in-flight fetch:
	// instead of every scrape performing its own XML-RPC round trip (each taking
	// up to rpcTimeout), callers that arrive while a fetch is running wait for
	// that one fetch and share its result.
	fetchGroup singleflight.Group

	processInfoDesc = prometheus.NewDesc(
		"supervisor_process_info",
		"Supervisor process information",
		[]string{"name", "group", "state", "exit_status"},
		nil,
	)
	processUptimeDesc = prometheus.NewDesc(
		"supervisor_process_uptime",
		"Uptime of Supervisor processes",
		[]string{"name", "group"},
		nil,
	)
	supervisordUpDesc = prometheus.NewDesc(
		"supervisord_up",
		"Supervisord XML-RPC connection status (1 if up, 0 if down)",
		nil,
		nil,
	)

	// snapshotMu guards currentSnapshot. A scrape publishes a brand new *snapshot
	// built entirely off to the side and only then swaps the pointer in, so a
	// concurrent Collect() always sees either the previous fully-formed snapshot
	// or the new one — never a partially-populated one.
	snapshotMu      sync.RWMutex
	currentSnapshot = &snapshot{}
)

// processSample is one process's data as published to Prometheus.
type processSample struct {
	name, group, state string
	exitStatus         int64
	running            bool
	uptimeSeconds      float64
	hasUptime          bool
}

// snapshot is the full, self-consistent set of data for one scrape.
type snapshot struct {
	up        float64
	processes []processSample
}

func init() {
	flag.StringVar(&supervisordURL, "supervisord-url", "http://localhost:9001/RPC2", "Supervisord XML-RPC URL (supports http:// and unix:// schemes)")
	flag.StringVar(&listenAddress, "web.listen-address", ":9876", "Address to listen for HTTP requests")
	flag.StringVar(&metricsPath, "web.telemetry-path", "/metrics", "Path under which to expose metrics")
	flag.StringVar(&username, "username", "", "Username for Supervisord authentication (prefer SUPERVISORD_USERNAME env var)")
	flag.StringVar(&password, "password", "", "Password for Supervisord authentication (prefer SUPERVISORD_PASSWORD env var to avoid leaking it via process listings)")
	flag.DurationVar(&rpcTimeout, "supervisord-timeout", 10*time.Second, "Timeout for XML-RPC requests to Supervisord")
	flag.BoolVar(&version, "version", false, "Displays application version")

	prometheus.MustRegister(supervisorCollector{})
}

// supervisorCollector implements prometheus.Collector by serving the latest
// atomically-swapped snapshot. Unlike mutating a registered GaugeVec via
// Reset()+repopulate, this can never expose a torn/partial update to a
// concurrent scrape.
type supervisorCollector struct{}

func (supervisorCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- processInfoDesc
	ch <- processUptimeDesc
	ch <- supervisordUpDesc
}

func (supervisorCollector) Collect(ch chan<- prometheus.Metric) {
	snapshotMu.RLock()
	snap := currentSnapshot
	snapshotMu.RUnlock()

	ch <- prometheus.MustNewConstMetric(supervisordUpDesc, prometheus.GaugeValue, snap.up)

	for _, p := range snap.processes {
		value := 0.0
		if p.running {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(processInfoDesc, prometheus.GaugeValue, value, p.name, p.group, p.state, strconv.FormatInt(p.exitStatus, 10))
		if p.running && p.hasUptime {
			ch <- prometheus.MustNewConstMetric(processUptimeDesc, prometheus.GaugeValue, p.uptimeSeconds, p.name, p.group)
		}
	}
}

func publishSnapshot(snap *snapshot) {
	snapshotMu.Lock()
	currentSnapshot = snap
	snapshotMu.Unlock()
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
// It is called once at startup; the returned transport (and the *xmlrpc.Client built on top of
// it) is reused across scrapes so repeated scrapes benefit from connection reuse instead of
// paying a fresh handshake every time. Its connections live for the process's lifetime — there's
// no explicit idle-connection cleanup because there's nothing to clean up until the process exits.
func createTransport(targetURL string) (http.RoundTripper, string, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse URL: %v", err)
	}

	var baseTransport *http.Transport
	clientURL := targetURL

	if parsedURL.Scheme == "unix" {
		// A unix:// URL must use three slashes (unix:///path/to.sock) so the whole
		// path lands in parsedURL.Path; with only two slashes, url.Parse silently
		// puts the first path segment into Host instead, which would dial the
		// wrong socket without any error.
		if parsedURL.Host != "" {
			return nil, "", fmt.Errorf("invalid unix socket URL %q: use unix:///path/to/socket (three slashes)", targetURL)
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
	return &timeoutTransport{Transport: transport, Timeout: rpcTimeout}, clientURL, nil
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

// refreshMetrics fetches the latest process info from Supervisord and publishes it, coalescing
// concurrent scrapes via fetchGroup so they share a single fetch instead of each performing their
// own. singleflight.Group also guarantees that a panic inside the fetch propagates to every
// waiting caller instead of leaving the group permanently stuck.
func refreshMetrics() {
	fetchGroup.Do("fetch", func() (interface{}, error) {
		fetchSupervisorProcessInfo()
		return nil, nil
	})
}

func fetchSupervisorProcessInfo() {
	result := []map[string]interface{}{}
	if err := rpcClient.Call("supervisor.getAllProcessInfo", nil, &result); err != nil {
		log.Printf("Error calling Supervisor XML-RPC method: %v", err)
		publishSnapshot(&snapshot{})
		return
	}

	// Create a map to store the latest process information for each unique combination of name and group
	latestInfo := make(map[processKey]map[string]interface{})

	for _, data := range result {
		name, _ := data["name"].(string)
		group, _ := data["group"].(string)

		key := processKey{name: name, group: group}

		existing, ok := latestInfo[key]
		if !ok {
			latestInfo[key] = data
			continue
		}

		// Compare timestamps to determine which information is more recent
		existingStartTime, existingOk := existing["start"].(int64)
		newStartTime, newOk := data["start"].(int64)

		switch {
		case newOk && (!existingOk || newStartTime > existingStartTime):
			latestInfo[key] = data
		case existingOk && newOk && newStartTime == existingStartTime:
			log.Printf("Warning: duplicate process entries for %s/%s share the same start time; keeping the first one encountered", name, group)
		case !existingOk && !newOk:
			log.Printf("Warning: duplicate process entries for %s/%s have no valid start time; keeping the first one encountered", name, group)
		}
	}

	processes := make([]processSample, 0, len(latestInfo))

	for _, data := range latestInfo {
		name, _ := data["name"].(string)
		group, _ := data["group"].(string)
		state, _ := data["statename"].(string)
		// kolo/xmlrpc decodes XML-RPC <int> values into interface{} as int64, not int.
		exitStatus, _ := data["exitstatus"].(int64)
		startTime, startTimeOk := data["start"].(int64)

		sample := processSample{
			name:       name,
			group:      group,
			state:      state,
			exitStatus: exitStatus,
			running:    state == "RUNNING",
		}

		if sample.running {
			if startTimeOk {
				sample.uptimeSeconds = float64(time.Now().Unix() - startTime)
				sample.hasUptime = true
			} else {
				log.Printf("Warning: process %s/%s is RUNNING but has no valid start time; skipping uptime metric", name, group)
			}
		}

		processes = append(processes, sample)
	}

	publishSnapshot(&snapshot{up: 1, processes: processes})
}

var promHandler = promhttp.Handler()

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	refreshMetrics()
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
		fmt.Printf("Supervisor Exporter v%s\n", appVersion)
		os.Exit(0)
	}

	// Built once and reused for every scrape; supervisordURL/username/password/rpcTimeout
	// are fixed after flag parsing, so there's no reason to rebuild these per request.
	var err error
	rpcTransport, rpcClientURL, err = createTransport(supervisordURL)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
	rpcClient, err = xmlrpc.NewClient(rpcClientURL, rpcTransport)
	if err != nil {
		fmt.Printf("Error creating Supervisor XML-RPC client: %s\n", err)
		os.Exit(1)
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
