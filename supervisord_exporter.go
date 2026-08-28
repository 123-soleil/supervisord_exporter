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
	"sync/atomic"
	"time"

	"github.com/kolo/xmlrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"
)

var (
	supervisordURL   string
	listenAddress    string
	metricsPath      string
	username         string
	password         string
	rpcTimeout       time.Duration
	staleGracePeriod time.Duration
	version          bool
	appVersion       = "0.1"

	// rpcTransport and rpcClientURL are built once at startup from the (static)
	// -supervisord-url/-username/-password/-supervisord-timeout flags and reused
	// across scrapes, so the underlying connection pool is shared instead of
	// starting cold every scrape. The *xmlrpc.Client itself is deliberately
	// rebuilt on every fetch (see fetchSupervisorProcessInfo): net/rpc.Client
	// permanently shuts itself down after a single decode error from its
	// underlying codec, and kolo/xmlrpc's reflective decoder can plausibly fail
	// on a truncated or unexpected response — a long-lived, reused *xmlrpc.Client
	// would then wedge the exporter (supervisord_up stuck at 0) until restart.
	rpcTransport http.RoundTripper
	rpcClientURL string

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
	lastSuccessDesc = prometheus.NewDesc(
		"supervisor_last_successful_scrape_timestamp_seconds",
		"Unix timestamp of the last successful Supervisord XML-RPC scrape",
		nil,
		nil,
	)

	// currentSnapshot is swapped atomically: a scrape builds a brand new *snapshot
	// entirely off to the side and only then stores the pointer, so a concurrent
	// Collect() always sees either the previous fully-formed snapshot or the new
	// one — never a partially-populated one.
	currentSnapshot atomic.Pointer[snapshot]
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
	// lastSuccess is zero until the first successful fetch. It's compared with
	// time.Since (not a stored Unix timestamp) so the grace-period countdown rides
	// on the monotonic clock reading time.Now() attaches, immune to wall-clock
	// steps (NTP correction, VM suspend/resume) — only the exported metric below
	// converts it to a wall-clock Unix timestamp, for external consumers.
	lastSuccess time.Time
}

func init() {
	flag.StringVar(&supervisordURL, "supervisord-url", "http://localhost:9001/RPC2", "Supervisord XML-RPC URL (supports http:// and unix:// schemes)")
	flag.StringVar(&listenAddress, "web.listen-address", ":9876", "Address to listen for HTTP requests")
	flag.StringVar(&metricsPath, "web.telemetry-path", "/metrics", "Path under which to expose metrics")
	flag.StringVar(&username, "username", "", "Username for Supervisord authentication (prefer SUPERVISORD_USERNAME env var)")
	flag.StringVar(&password, "password", "", "Password for Supervisord authentication (prefer SUPERVISORD_PASSWORD env var to avoid leaking it via process listings)")
	flag.DurationVar(&rpcTimeout, "supervisord-timeout", 10*time.Second, "Timeout for XML-RPC requests to Supervisord")
	flag.DurationVar(&staleGracePeriod, "stale-grace-period", time.Minute, "How long to keep serving the last known process metrics (with supervisord_up=0) after Supervisord becomes unreachable, before clearing them as too stale to trust. It's only re-evaluated on each scrape, so set it well above your Prometheus scrape_interval or it won't tolerate even one hiccup; 0 disables the grace period entirely")
	flag.BoolVar(&version, "version", false, "Displays application version")

	currentSnapshot.Store(&snapshot{})
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
	ch <- lastSuccessDesc
}

func (supervisorCollector) Collect(ch chan<- prometheus.Metric) {
	snap := currentSnapshot.Load()

	ch <- prometheus.MustNewConstMetric(supervisordUpDesc, prometheus.GaugeValue, snap.up)
	if !snap.lastSuccess.IsZero() {
		ch <- prometheus.MustNewConstMetric(lastSuccessDesc, prometheus.GaugeValue, float64(snap.lastSuccess.Unix()))
	}

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
	currentSnapshot.Store(snap)
}

// markDown flags the Supervisord connection as down. It keeps serving the last known
// process list for up to staleGracePeriod after the last successful fetch, so a brief
// RPC hiccup doesn't make every process's metrics vanish for that scrape — only
// supervisord_up drops. Once the outage outlasts the grace period, the process list is
// cleared: serving arbitrarily old process/uptime data during a real, sustained outage
// would be actively misleading (frozen uptimes, processes that may no longer exist).
// supervisor_last_successful_scrape_timestamp_seconds lets consumers detect staleness
// directly instead of having to remember to also check supervisord_up.
func markDown() {
	prev := currentSnapshot.Load()
	processes := prev.processes
	// The processes != nil guard makes this a one-time transition: once a sustained
	// outage has already cleared the list, every later failed scrape sees processes
	// already nil and skips straight past, instead of re-logging this warning (and
	// redundantly re-nil'ing an already-nil list) on every single scrape for the
	// rest of the outage.
	if processes != nil && !prev.lastSuccess.IsZero() && time.Since(prev.lastSuccess) > staleGracePeriod {
		log.Printf("Warning: no successful Supervisord scrape in over %s (stale-grace-period); clearing previously reported process metrics", staleGracePeriod)
		processes = nil
	}
	currentSnapshot.Store(&snapshot{up: 0, processes: processes, lastSuccess: prev.lastSuccess})
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
		// For Unix sockets, clone the default transport (for its tuned defaults —
		// MaxIdleConns, IdleConnTimeout, etc.) and just override how it dials.
		socketPath := parsedURL.Path
		baseTransport = http.DefaultTransport.(*http.Transport).Clone()
		baseTransport.DialContext = func(ctx context.Context, proto, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
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
	client, err := xmlrpc.NewClient(rpcClientURL, rpcTransport)
	if err != nil {
		log.Printf("Error creating Supervisor XML-RPC client: %v", err)
		markDown()
		return
	}
	defer client.Close()

	result := []map[string]interface{}{}
	if err := client.Call("supervisor.getAllProcessInfo", nil, &result); err != nil {
		log.Printf("Error calling Supervisor XML-RPC method: %v", err)
		markDown()
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

	// Captured once so every metric published from this scrape (each process's
	// uptime and the snapshot's lastSuccess) reflects the same instant, rather
	// than drifting apart across a loop over many processes.
	now := time.Now()

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
				uptime := now.Unix() - startTime
				if uptime < 0 {
					log.Printf("Warning: process %s/%s has a start time in the future (clock skew?); clamping uptime to 0", name, group)
					uptime = 0
				}
				sample.uptimeSeconds = float64(uptime)
				sample.hasUptime = true
			} else {
				log.Printf("Warning: process %s/%s is RUNNING but has no valid start time; skipping uptime metric", name, group)
			}
		}

		processes = append(processes, sample)
	}

	publishSnapshot(&snapshot{up: 1, processes: processes, lastSuccess: now})
}

var promHandler = promhttp.Handler()

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	refreshMetrics()
	promHandler.ServeHTTP(w, r)
}

func main() {
	flag.Parse()

	// -version short-circuits everything else, including flag validation below:
	// it's expected to work even if other flags are missing or invalid.
	if version {
		fmt.Printf("Supervisor Exporter v%s\n", appVersion)
		os.Exit(0)
	}

	if staleGracePeriod < 0 {
		log.Fatalf("Error: -stale-grace-period must be >= 0, got %s", staleGracePeriod)
	}

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

	// Built once and reused for every scrape; supervisordURL/username/password/rpcTimeout
	// are fixed after flag parsing, so there's no reason to rebuild the transport per request.
	var err error
	rpcTransport, rpcClientURL, err = createTransport(supervisordURL)
	if err != nil {
		log.Fatalf("Error creating transport: %v", err)
	}

	http.HandleFunc(metricsPath, metricsHandler)

	server := &http.Server{
		Addr:              listenAddress,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      rpcTimeout + 10*time.Second,
	}

	log.Printf("Listening on %s", listenAddress)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Error: %s", err)
	}
}
