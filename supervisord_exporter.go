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
	appVersion       = "0.4"

	// rpcTransport and rpcClientURL are built once at startup from the (static)
	// -supervisord-url/-username/-password/-supervisord-timeout flags and reused
	// across scrapes, so the underlying connection pool is shared instead of
	// starting cold every scrape. The *xmlrpc.Client itself is deliberately
	// rebuilt on every fetch (see fetchSupervisorProcessInfo): net/rpc.Client
	// permanently shuts itself down after a single decode error from its
	// underlying codec, and kolo/xmlrpc's reflective decoder can plausibly fail
	// on a truncated or unexpected response — a long-lived, reused *xmlrpc.Client
	// would then wedge the exporter (supervisord_up stuck at 0) until restart.
	//
	// IMPORTANT: rpcTransport's outermost concrete type must never be a bare
	// *http.Transport. kolo/xmlrpc's per-fetch Client.Close() only calls
	// CloseIdleConnections() when it can type-assert the transport straight to
	// *http.Transport; since rpcTransport is always wrapped (at minimum in
	// *timeoutTransport), that assertion never matches, Close() leaves the
	// pool's connections alone, and they survive to be reused by the next
	// fetch. Passing the raw *http.Transport to xmlrpc.NewClient instead would
	// make that assertion succeed and silently defeat this connection reuse —
	// every scrape would then pay a fresh handshake, with no error to reveal why.
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
	// staleCleared is an explicit "already cleared for staleness" flag, rather
	// than inferring that state from processes == nil. It's true only once
	// markDown has cleared processes after the grace period expired, and is
	// always false again on the next successful fetch. Keeping it explicit means
	// markDown's one-shot logging doesn't depend on fetchSupervisorProcessInfo
	// happening to build processes via make(..., 0, n) (never nil, even when
	// empty) — a future change to that construction couldn't silently make
	// markDown think every post-outage failure is "never succeeded" again.
	staleCleared bool
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
	staleCleared := prev.staleCleared
	// The staleCleared guard makes this a one-time transition: once a sustained
	// outage has already cleared the list, every later failed scrape sees
	// staleCleared already true and skips straight past, instead of re-logging
	// this warning (and redundantly re-nil'ing an already-nil list) on every
	// single scrape for the rest of the outage. The lastSuccess.IsZero() check
	// is still needed here (unlike staleCleared, it isn't implied by anything
	// else): without it, a Supervisord that has never once succeeded would tick
	// over from a zero lastSuccess (year 1) as already "stale" on the very first
	// failed scrape, logging a misleading "no successful scrape in over X"
	// before there was ever a successful one to go stale.
	if !staleCleared && !prev.lastSuccess.IsZero() && time.Since(prev.lastSuccess) > staleGracePeriod {
		log.Printf("Warning: no successful Supervisord scrape in over %s (stale-grace-period); clearing previously reported process metrics", staleGracePeriod)
		processes = nil
		staleCleared = true
	}
	currentSnapshot.Store(&snapshot{up: 0, processes: processes, lastSuccess: prev.lastSuccess, staleCleared: staleCleared})
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
// authConfigured reports whether both Supervisord credentials were provided.
// It's the single source of truth for "is auth on" — createTransport uses it to
// decide whether to wrap the transport, and it's paired with
// authPartiallyConfigured below so the two conditions can't silently drift
// apart from each other.
func authConfigured() bool {
	return username != "" && password != ""
}

// authPartiallyConfigured reports whether exactly one of username/password was
// provided — almost certainly a misconfiguration, since authConfigured() is
// then false and auth ends up silently disabled.
func authPartiallyConfigured() bool {
	return (username != "") != (password != "")
}

func createTransport(targetURL string) (http.RoundTripper, string, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse URL: %v", err)
	}

	// Cloned once here (rather than duplicated in each scheme branch below) for
	// its tuned defaults — MaxIdleConns, IdleConnTimeout, etc. — which the unix
	// branch then overrides only how it dials.
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	clientURL := targetURL

	switch parsedURL.Scheme {
	case "unix":
		// A unix:// URL must use three slashes (unix:///path/to.sock) so the whole
		// path lands in parsedURL.Path; with only two slashes, url.Parse silently
		// puts the first path segment into Host instead, which would dial the
		// wrong socket without any error.
		if parsedURL.Host != "" {
			return nil, "", fmt.Errorf("invalid unix socket URL %q: use unix:///path/to/socket (three slashes)", targetURL)
		}
		if parsedURL.Path == "" {
			if parsedURL.Opaque != "" {
				// e.g. "unix:var/run/x.sock" (no "//"): url.Parse puts the given
				// path in Opaque instead of Path, so point at the actual fix
				// rather than claiming no path was given at all.
				return nil, "", fmt.Errorf("invalid unix socket URL %q: use unix:///%s (three slashes)", targetURL, parsedURL.Opaque)
			}
			return nil, "", fmt.Errorf("invalid unix socket URL %q: missing socket path", targetURL)
		}
		socketPath := parsedURL.Path
		baseTransport.DialContext = func(ctx context.Context, proto, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		}
		// Return a fake HTTP URL for the xmlrpc client, the transport will handle the actual connection
		clientURL = "http://localhost/RPC2"
	case "http", "https":
		// Hostname() (not Host) so a port-only host like "http://:9001/RPC2" — a
		// plausible typo for "http://localhost:9001/RPC2" — is caught too: Host
		// would be ":9001" (non-empty) there, while Hostname() is correctly "".
		if parsedURL.Hostname() == "" {
			return nil, "", fmt.Errorf("invalid -supervisord-url %q: missing host", targetURL)
		}
	default:
		// Including a missing/misparsed scheme from a typo like "localhost:9001/RPC2"
		// (no "http://"), which url.Parse would otherwise silently accept as scheme
		// "localhost" — rejected here rather than falling through to the HTTP path
		// and only failing cryptically on the first scrape.
		return nil, "", fmt.Errorf("invalid -supervisord-url %q: unsupported scheme %q (expected http, https, or unix)", targetURL, parsedURL.Scheme)
	}

	var transport http.RoundTripper = baseTransport

	// Apply authentication if credentials are provided
	if authConfigured() {
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
		name, nameOk := data["name"].(string)
		group, groupOk := data["group"].(string)
		if !nameOk || !groupOk {
			// Falling back to "" lets this entry still be processed instead of
			// dropped outright, but it means two malformed entries missing the
			// same field can collide onto the same processKey and silently lose
			// one of them in the dedup below — log it so that's diagnosable.
			log.Printf("Warning: Supervisord process entry has a missing/invalid name or group (name=%q group=%q); it may collide with another malformed entry", name, group)
		}

		key := processKey{name: name, group: group}

		existing, ok := latestInfo[key]
		if !ok {
			latestInfo[key] = data
			continue
		}

		// Compare timestamps to determine which information is more recent
		existingStartTime, existingOk := existing["start"].(int64)
		newStartTime, newOk := data["start"].(int64)

		// Any case that isn't a clear "the new entry is more recent" replacement is
		// logged here, whichever way the start times (or their absence) compare —
		// the default case covers any combination the specific ones below don't,
		// so no combination of duplicate entries ever falls through silently.
		switch {
		case newOk && (!existingOk || newStartTime > existingStartTime):
			latestInfo[key] = data
		case existingOk && newOk && newStartTime == existingStartTime:
			log.Printf("Warning: duplicate process entries for %s/%s share the same start time; keeping the one already seen", name, group)
		case !existingOk && !newOk:
			log.Printf("Warning: duplicate process entries for %s/%s have no valid start time; keeping the one already seen", name, group)
		default:
			log.Printf("Warning: duplicate process entries for %s/%s; keeping the one already seen", name, group)
		}
	}

	// Captured once so every metric published from this scrape (each process's
	// uptime and the snapshot's lastSuccess) reflects the same instant, rather
	// than drifting apart across a loop over many processes.
	now := time.Now()

	processes := make([]processSample, 0, len(latestInfo))

	for key, data := range latestInfo {
		// key.name/key.group are already the validated, dedup-key values from the
		// loop above — re-asserting data["name"]/data["group"] here would just
		// repeat that same extraction a second time.
		state, _ := data["statename"].(string)
		// kolo/xmlrpc decodes XML-RPC <int> values into interface{} as int64, not int.
		exitStatus, _ := data["exitstatus"].(int64)
		startTime, startTimeOk := data["start"].(int64)

		sample := processSample{
			name:       key.name,
			group:      key.group,
			state:      state,
			exitStatus: exitStatus,
			running:    state == "RUNNING",
		}

		if sample.running {
			if startTimeOk {
				uptime := now.Unix() - startTime
				if uptime < 0 {
					log.Printf("Warning: process %s/%s has a start time in the future (clock skew?); clamping uptime to 0", key.name, key.group)
					uptime = 0
				}
				sample.uptimeSeconds = float64(uptime)
				sample.hasUptime = true
			} else {
				log.Printf("Warning: process %s/%s is RUNNING but has no valid start time; skipping uptime metric", key.name, key.group)
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
	if rpcTimeout <= 0 {
		// Unlike -stale-grace-period, 0 has no sensible meaning here: a zero (or
		// negative) context.WithTimeout deadline is already expired before the
		// request is even sent, so every scrape would fail instantly with no
		// indication why, and supervisord_up would be stuck at 0 permanently.
		log.Fatalf("Error: -supervisord-timeout must be > 0, got %s", rpcTimeout)
	}

	// Prefer environment variables for credentials over CLI flags, which are
	// visible to other local users via the process list (e.g. `ps aux`).
	if v := os.Getenv("SUPERVISORD_USERNAME"); v != "" {
		username = v
	}
	if v := os.Getenv("SUPERVISORD_PASSWORD"); v != "" {
		password = v
	}
	if authPartiallyConfigured() {
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
