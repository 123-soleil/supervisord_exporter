package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// syncBuffer is a concurrency-safe bytes.Buffer. It's needed because
// exporterProc points both cmd.Stdout and cmd.Stderr at the same buffer:
// os/exec copies each stream on its own goroutine, so without locking, two
// writes (stdout vs stderr) — or a test reading via String() while either is
// still writing — would race on a plain bytes.Buffer, which isn't safe for
// concurrent use.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// binPath is the path to the exporter binary built once in TestMain, so every
// test below exercises the actual compiled artifact (a post-build/integration
// test) rather than calling internal functions directly.
var binPath string

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "supervisord_exporter_test")
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to create temp dir:", err)
		os.Exit(1)
	}
	// Note: os.Exit below skips defers, so tmpDir is removed explicitly on every
	// exit path instead of via `defer os.RemoveAll(tmpDir)`.

	binPath = filepath.Join(tmpDir, "supervisord_exporter")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "failed to build exporter binary:", err)
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// --- helpers: fake Supervisord XML-RPC server ---

type fakeProcess struct {
	Name, Group, State string
	ExitStatus         int
	Start              int64
	OmitStart          bool // simulate a response missing the "start" member
}

// buildGetAllProcessInfoXML renders a supervisor.getAllProcessInfo XML-RPC response
// body for the given processes.
func buildGetAllProcessInfoXML(procs []fakeProcess) []byte {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0"?><methodResponse><params><param><value><array><data>`)
	for _, p := range procs {
		sb.WriteString(`<value><struct>`)
		fmt.Fprintf(&sb, `<member><name>name</name><value><string>%s</string></value></member>`, p.Name)
		fmt.Fprintf(&sb, `<member><name>group</name><value><string>%s</string></value></member>`, p.Group)
		fmt.Fprintf(&sb, `<member><name>statename</name><value><string>%s</string></value></member>`, p.State)
		fmt.Fprintf(&sb, `<member><name>exitstatus</name><value><int>%d</int></value></member>`, p.ExitStatus)
		if !p.OmitStart {
			fmt.Fprintf(&sb, `<member><name>start</name><value><int>%d</int></value></member>`, p.Start)
		}
		sb.WriteString(`</struct></value>`)
	}
	sb.WriteString(`</data></array></value></param></params></methodResponse>`)
	return []byte(sb.String())
}

// fakeSupervisord starts an httptest server that always answers /RPC2 with the
// current body served by getBody() — a func pointer so tests can swap the
// response (e.g. to simulate an outage) after the server has started.
func fakeSupervisord(t *testing.T, getBody func() ([]byte, int)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, status := getBody()
		if status != 0 && status != http.StatusOK {
			http.Error(w, "simulated failure", status)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func staticBody(body []byte) func() ([]byte, int) {
	return func() ([]byte, int) { return body, http.StatusOK }
}

// --- helpers: running the actual built binary ---

type exporterProc struct {
	cmd    *exec.Cmd
	addr   string
	stderr *syncBuffer
}

// freeAddr reserves a free TCP port by binding then immediately releasing it.
// There's a small window where another process could grab it first, but this
// is a standard, low-risk pattern for test scaffolding.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve a free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// startExporter launches the built binary with the given extra flags, on a
// freshly reserved listen address, and arranges for it to be killed at the
// end of the test.
func startExporter(t *testing.T, extraArgs ...string) *exporterProc {
	t.Helper()
	listenAddr := freeAddr(t)
	args := append([]string{"-web.listen-address=" + listenAddr}, extraArgs...)

	cmd := exec.Command(binPath, args...)
	out := &syncBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start exporter: %v", err)
	}

	ep := &exporterProc{cmd: cmd, addr: listenAddr, stderr: out}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	return ep
}

func (e *exporterProc) url(scheme string) string {
	return scheme + "://" + e.addr + "/metrics"
}

// waitReady polls /metrics until it responds or the timeout elapses, failing
// the test (with the process's captured output for context) if it never comes up.
func (e *exporterProc) waitReady(t *testing.T, scheme string, client *http.Client, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(e.url(scheme))
		if err == nil {
			resp.Body.Close()
			return
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("exporter never became ready: %v\noutput so far:\n%s", lastErr, e.stderr.String())
}

func (e *exporterProc) metrics(t *testing.T, scheme string, client *http.Client) string {
	t.Helper()
	resp, err := client.Get(e.url(scheme))
	if err != nil {
		t.Fatalf("GET %s failed: %v\noutput so far:\n%s", e.url(scheme), err, e.stderr.String())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body failed: %v", err)
	}
	return string(body)
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected output to contain %q, got:\n%s", needle, haystack)
	}
}

func assertNotContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Errorf("expected output NOT to contain %q, got:\n%s", needle, haystack)
	}
}

// --- tests ---

func TestMetrics_RunningAndExitedProcess(t *testing.T) {
	body := buildGetAllProcessInfoXML([]fakeProcess{
		{Name: "myproc", Group: "mygroup", State: "EXITED", ExitStatus: 137, Start: 1700000000},
		{Name: "runner", Group: "rungroup", State: "RUNNING", ExitStatus: 0, Start: 1700000000},
	})
	supervisord := fakeSupervisord(t, staticBody(body))

	ep := startExporter(t, "-supervisord-url="+supervisord.URL+"/RPC2")
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)

	// Regression check for the historic bug where kolo/xmlrpc decodes XML-RPC
	// <int> as int64, and a `.(int)` type assertion silently defaulted
	// exit_status to "0" for every process, running or not.
	assertContains(t, out, `supervisor_process_info{exit_status="137",group="mygroup",name="myproc",state="EXITED"} 0`)
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="rungroup",name="runner",state="RUNNING"} 1`)
	assertContains(t, out, "supervisord_up 1")
	assertContains(t, out, `supervisor_process_uptime{group="rungroup",name="runner"}`)
	assertContains(t, out, "supervisor_last_successful_scrape_timestamp_seconds")
	// The EXITED process isn't running, so no uptime series for it.
	assertNotContains(t, out, `supervisor_process_uptime{group="mygroup",name="myproc"}`)
}

func TestMetrics_DuplicateProcessEntries_Dedup(t *testing.T) {
	body := buildGetAllProcessInfoXML([]fakeProcess{
		// Same (name, group): the later start time should win over the earlier one.
		{Name: "dup", Group: "dupgroup", State: "STOPPED", ExitStatus: 1, Start: 1000},
		{Name: "dup", Group: "dupgroup", State: "RUNNING", ExitStatus: 0, Start: 2000},
		// Same (name, group), identical start times: an ambiguous duplicate that
		// should log a warning and keep the first one seen.
		{Name: "tie", Group: "tiegroup", State: "RUNNING", ExitStatus: 0, Start: 5000},
		{Name: "tie", Group: "tiegroup", State: "RUNNING", ExitStatus: 0, Start: 5000},
		// Same (name, group), neither with a valid start time: also ambiguous.
		{Name: "nostart", Group: "nostartgroup", State: "STOPPED", ExitStatus: 0, OmitStart: true},
		{Name: "nostart", Group: "nostartgroup", State: "STOPPED", ExitStatus: 0, OmitStart: true},
	})
	supervisord := fakeSupervisord(t, staticBody(body))

	ep := startExporter(t, "-supervisord-url="+supervisord.URL+"/RPC2")
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)

	// Exactly one series per duplicated (name, group) pair — the "losing" entry
	// must not also show up.
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="dupgroup",name="dup",state="RUNNING"} 1`)
	assertNotContains(t, out, `group="dupgroup",name="dup",state="STOPPED"`)
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="tiegroup",name="tie",state="RUNNING"} 1`)
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="nostartgroup",name="nostart",state="STOPPED"} 0`)

	// The two ambiguous-duplicate cases should each have logged their own
	// specific warning, not just been silently resolved.
	logs := ep.stderr.String()
	assertContains(t, logs, "share the same start time")
	assertContains(t, logs, "have no valid start time")
}

func TestMetrics_RunningProcessMissingStartTime(t *testing.T) {
	body := buildGetAllProcessInfoXML([]fakeProcess{
		{Name: "nostartrunner", Group: "g", State: "RUNNING", ExitStatus: 0, OmitStart: true},
	})
	supervisord := fakeSupervisord(t, staticBody(body))

	ep := startExporter(t, "-supervisord-url="+supervisord.URL+"/RPC2")
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="g",name="nostartrunner",state="RUNNING"} 1`)
	// No start time means no uptime can be computed — the series must be absent
	// entirely, not a bogus value like 0 or a huge number.
	assertNotContains(t, out, `supervisor_process_uptime{group="g",name="nostartrunner"}`)
	assertContains(t, ep.stderr.String(), "is RUNNING but has no valid start time")
}

func TestMetrics_UnixSocket(t *testing.T) {
	// Deliberately not under t.TempDir(): that nests several directory levels
	// under the OS temp dir, which on some platforms (e.g. macOS's
	// /var/folders/.../T/...) can push the socket path close to or past
	// AF_UNIX's sun_path limit (~104-108 bytes). os.TempDir() directly plus a
	// short, unique filename keeps this portable.
	sockPath := filepath.Join(os.TempDir(), fmt.Sprintf("se_test_%d.sock", os.Getpid()))
	t.Cleanup(func() { os.Remove(sockPath) })
	body := buildGetAllProcessInfoXML([]fakeProcess{
		{Name: "runner", Group: "g", State: "RUNNING", ExitStatus: 0, Start: 1700000000},
	})

	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on unix socket: %v", err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	})}
	go srv.Serve(l) //nolint:errcheck // closed via t.Cleanup below
	t.Cleanup(func() { srv.Close() })

	// Exercises the exporter talking to Supervisord over a real unix domain
	// socket end-to-end (every other test uses httptest's TCP listener), so a
	// regression in the unix DialContext override or the fake client URL
	// substitution wouldn't otherwise be caught.
	ep := startExporter(t, "-supervisord-url=unix://"+sockPath)
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 1")
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="g",name="runner",state="RUNNING"} 1`)
}

func TestBasicAuth_CorrectCredentialsSucceed(t *testing.T) {
	const user, pass = "dummy", "s3cret"
	body := buildGetAllProcessInfoXML([]fakeProcess{
		{Name: "runner", Group: "g", State: "RUNNING", ExitStatus: 0, Start: 1700000000},
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, ok := r.BasicAuth()
		if !ok || gotUser != user || gotPass != pass {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write(body)
	}))
	t.Cleanup(srv.Close)

	ep := startExporter(t,
		"-supervisord-url="+srv.URL+"/RPC2",
		"-username="+user,
		"-password="+pass,
	)
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 1")
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="g",name="runner",state="RUNNING"} 1`)
}

func TestBasicAuth_WrongCredentialsFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, ok := r.BasicAuth()
		if !ok || gotUser != "dummy" || gotPass != "correct" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write(buildGetAllProcessInfoXML(nil))
	}))
	t.Cleanup(srv.Close)

	ep := startExporter(t,
		"-supervisord-url="+srv.URL+"/RPC2",
		"-username=dummy",
		"-password=wrong",
	)
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 0")
}

func TestMetrics_SupervisordUnreachable(t *testing.T) {
	closedAddr := freeAddr(t) // nothing listens here

	ep := startExporter(t,
		"-supervisord-url=http://"+closedAddr+"/RPC2",
		"-supervisord-timeout=1s",
	)
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 0")
	assertNotContains(t, out, "supervisor_process_info{")
}

func TestMetrics_StaleGracePeriod_RetainThenClear(t *testing.T) {
	var down atomic.Bool
	body := buildGetAllProcessInfoXML([]fakeProcess{
		{Name: "runner", Group: "rungroup", State: "RUNNING", ExitStatus: 0, Start: 1700000000},
	})
	supervisord := fakeSupervisord(t, func() ([]byte, int) {
		if down.Load() {
			return nil, http.StatusInternalServerError
		}
		return body, http.StatusOK
	})

	ep := startExporter(t,
		"-supervisord-url="+supervisord.URL+"/RPC2",
		"-supervisord-timeout=1s",
		// Generous relative to the check-immediately-after-down assertion below,
		// so scheduling/HTTP round-trip latency on a slow/loaded CI runner can't
		// make the grace period spuriously appear to have already elapsed.
		"-stale-grace-period=3s",
	)
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	// One successful scrape first, so there's something to retain.
	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 1")
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="rungroup",name="runner",state="RUNNING"} 1`)

	down.Store(true)

	// Immediately after going down, metrics should still be retained (within
	// the grace period) even though supervisord_up has already dropped.
	out = ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 0")
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="rungroup",name="runner",state="RUNNING"} 1`)

	// After the grace period elapses, the stale process metrics get cleared.
	deadline := time.Now().Add(10 * time.Second)
	for {
		out = ep.metrics(t, "http", client)
		if !strings.Contains(out, "supervisor_process_info{") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("process metrics were never cleared after the stale-grace-period elapsed:\n%s", out)
		}
		time.Sleep(100 * time.Millisecond)
	}
	assertContains(t, out, "supervisord_up 0")
}

func TestVersionFlag(t *testing.T) {
	cmd := exec.Command(binPath, "-version")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected -version to exit 0, got err=%v output=%s", err, out)
	}
	assertContains(t, string(out), "Supervisor Exporter v")
}

func TestInvalidConfigFailsFast(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		wantSubstr string
	}{
		{"missing scheme typo", []string{"-supervisord-url=localhost:9001/RPC2"}, "unsupported scheme"},
		{"unix url missing path", []string{"-supervisord-url=unix://"}, "missing socket path"},
		{"unix url two slashes", []string{"-supervisord-url=unix://var/run/x.sock"}, "three slashes"},
		{"http url missing host", []string{"-supervisord-url=http:///RPC2"}, "missing host"},
		{"http url port-only host", []string{"-supervisord-url=http://:9001/RPC2"}, "missing host"},
		{"negative stale grace period", []string{"-stale-grace-period=-1s"}, "must be >= 0"},
		{"zero timeout", []string{"-supervisord-timeout=0"}, "must be > 0"},
		{"negative timeout", []string{"-supervisord-timeout=-1s"}, "must be > 0"},
		{"tls cert without key", []string{"-web.tls-cert-file=/nonexistent.crt"}, "must both be set together"},
		{"tls client ca without cert/key", []string{"-web.tls-client-ca-file=/nonexistent.crt"}, "requires -web.tls-cert-file"},
		{"supervisord tls ca file missing", []string{"-supervisord-url=https://example.invalid/RPC2", "-supervisord-tls-ca-file=/nonexistent-ca.crt"}, "reading -supervisord-tls-ca-file"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-web.listen-address=127.0.0.1:0"}, tc.args...)
			cmd := exec.Command(binPath, args...)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected a non-zero exit, but the exporter started successfully. output:\n%s", out)
			}
			assertContains(t, string(out), tc.wantSubstr)
		})
	}
}

// --- helpers: ephemeral PKI for TLS/mTLS tests ---

type testPKI struct {
	caCertPEM     []byte
	serverCertPEM []byte
	serverKeyPEM  []byte
	clientCertPEM []byte
	clientKeyPEM  []byte
}

func generateTestPKI(t *testing.T) testPKI {
	t.Helper()

	encodeCert := func(der []byte) []byte {
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	}
	encodeKey := func(key *rsa.PrivateKey) []byte {
		return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating server certificate: %v", err)
	}

	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating client key: %v", err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "prometheus-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating client certificate: %v", err)
	}

	return testPKI{
		caCertPEM:     encodeCert(caDER),
		serverCertPEM: encodeCert(serverDER),
		serverKeyPEM:  encodeKey(serverKey),
		clientCertPEM: encodeCert(clientDER),
		clientKeyPEM:  encodeKey(clientKey),
	}
}

func writeTemp(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("writing %s: %v", p, err)
	}
	return p
}

func TestMetrics_SupervisordHTTPSWithCustomCA(t *testing.T) {
	pki := generateTestPKI(t)
	dir := t.TempDir()
	caPath := writeTemp(t, dir, "ca.crt", pki.caCertPEM)

	serverCert, err := tls.X509KeyPair(pki.serverCertPEM, pki.serverKeyPEM)
	if err != nil {
		t.Fatalf("loading server keypair: %v", err)
	}
	body := buildGetAllProcessInfoXML([]fakeProcess{
		{Name: "runner", Group: "g", State: "RUNNING", ExitStatus: 0, Start: 1700000000},
	})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(body)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// -supervisord-tls-ca-file trusts the private CA that signed the fake
	// Supervisord's server certificate, so the scrape should succeed even
	// though that CA isn't in the system trust store.
	ep := startExporter(t,
		"-supervisord-url="+srv.URL+"/RPC2",
		"-supervisord-tls-ca-file="+caPath,
	)
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 1")
	assertContains(t, out, `supervisor_process_info{exit_status="0",group="g",name="runner",state="RUNNING"} 1`)
}

func TestMetrics_SupervisordHTTPSWithoutTrustedCAFails(t *testing.T) {
	pki := generateTestPKI(t)
	serverCert, err := tls.X509KeyPair(pki.serverCertPEM, pki.serverKeyPEM)
	if err != nil {
		t.Fatalf("loading server keypair: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(buildGetAllProcessInfoXML(nil))
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// No -supervisord-tls-ca-file: the self-signed cert isn't in the system
	// trust store, so every scrape must fail rather than silently succeed.
	ep := startExporter(t, "-supervisord-url="+srv.URL+"/RPC2", "-supervisord-timeout=2s")
	client := &http.Client{Timeout: 5 * time.Second}
	ep.waitReady(t, "http", client, 5*time.Second)

	out := ep.metrics(t, "http", client)
	assertContains(t, out, "supervisord_up 0")
}

func TestTLS_PlainHTTPRejected_HTTPSAccepted(t *testing.T) {
	dir := t.TempDir()
	pki := generateTestPKI(t)
	certPath := writeTemp(t, dir, "server.crt", pki.serverCertPEM)
	keyPath := writeTemp(t, dir, "server.key", pki.serverKeyPEM)

	supervisord := fakeSupervisord(t, staticBody(buildGetAllProcessInfoXML(nil)))
	ep := startExporter(t,
		"-supervisord-url="+supervisord.URL+"/RPC2",
		"-web.tls-cert-file="+certPath,
		"-web.tls-key-file="+keyPath,
	)

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(pki.caCertPEM)
	httpsClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: caPool}},
	}
	ep.waitReady(t, "https", httpsClient, 5*time.Second)
	out := ep.metrics(t, "https", httpsClient)
	assertContains(t, out, "supervisord_up")

	plainClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := plainClient.Get(ep.url("http"))
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("expected a plain HTTP request against a TLS-only listener to fail, got 200 OK")
		}
	}
}

func TestMTLS_RequiresValidClientCert(t *testing.T) {
	dir := t.TempDir()
	pki := generateTestPKI(t)
	certPath := writeTemp(t, dir, "server.crt", pki.serverCertPEM)
	keyPath := writeTemp(t, dir, "server.key", pki.serverKeyPEM)
	caPath := writeTemp(t, dir, "ca.crt", pki.caCertPEM)

	supervisord := fakeSupervisord(t, staticBody(buildGetAllProcessInfoXML(nil)))
	ep := startExporter(t,
		"-supervisord-url="+supervisord.URL+"/RPC2",
		"-web.tls-cert-file="+certPath,
		"-web.tls-key-file="+keyPath,
		"-web.tls-client-ca-file="+caPath,
	)

	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(pki.caCertPEM)

	// Establish (and wait for) a working mTLS connection first, using a valid
	// client certificate. This confirms the server is actually up and accepting
	// TLS before we test rejection below — otherwise a "no client cert" request
	// failing merely because the listener isn't up yet would be indistinguishable
	// from an actual mTLS rejection, and the test could pass without ever
	// exercising the rejection path it claims to verify.
	clientCert, err := tls.X509KeyPair(pki.clientCertPEM, pki.clientKeyPEM)
	if err != nil {
		t.Fatalf("loading client keypair: %v", err)
	}
	withCertClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:      caPool,
			Certificates: []tls.Certificate{clientCert},
		}},
	}
	ep.waitReady(t, "https", withCertClient, 5*time.Second)

	// Now that the server is confirmed up and accepting valid mTLS connections,
	// a request without a client certificate must be rejected because of mTLS,
	// not because the server isn't listening yet.
	noCertClient := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: caPool}},
	}
	if resp, err := noCertClient.Get(ep.url("https")); err == nil {
		resp.Body.Close()
		t.Fatalf("expected a request without a client certificate to be rejected by mTLS, but it succeeded")
	}

	out := ep.metrics(t, "https", withCertClient)
	assertContains(t, out, "supervisord_up")
}
