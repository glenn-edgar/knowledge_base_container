package probes

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
)

// makeContainer builds a ContainerInfo with the given probe labels and
// the specified IP advertised on `bridge`.
func makeContainer(id, name, ip string, labels map[string]string) dockercli.ContainerInfo {
	ci := dockercli.ContainerInfo{
		ID:          id,
		Name:        name,
		State:       "running",
		Labels:      map[string]string{},
		IPAddresses: map[string]string{},
	}
	for k, v := range labels {
		ci.Labels[k] = v
	}
	if ip != "" {
		ci.IPAddresses["bridge"] = ip
	}
	return ci
}

// startTestServer spins up an httptest server on the loopback interface
// and returns (host, port, close). Always returns the given status on
// the given path.
func startTestServer(t *testing.T, path string, status int32) (string, int, *httptest.Server, *atomic.Int32) {
	t.Helper()
	statusVar := &atomic.Int32{}
	statusVar.Store(status)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(int(statusVar.Load()))
	}))
	host, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split host/port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	return host, port, srv, statusVar
}

func TestRunner_HappyPath_ProbeSucceeds(t *testing.T) {
	host, port, srv, _ := startTestServer(t, "/health", 200)
	defer srv.Close()

	router := newRouterFromSet("bridge")
	r := NewRunner(router)
	defer r.Stop()

	ci := makeContainer("idA", "test_app_01", host, map[string]string{
		"nanodatacenter.probe.exceptions_ui.path":          "/health",
		"nanodatacenter.probe.exceptions_ui.internal_port": strconv.Itoa(port),
		"nanodatacenter.probe.exceptions_ui.interval_s":    "1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Reconcile(ctx, []dockercli.ContainerInfo{ci})

	// Wait for the first probe to complete.
	if !waitForProbeOk(r, "idA", true, 2*time.Second) {
		t.Fatalf("expected probe to report ok=true within 2s")
	}

	annotated := []dockercli.ContainerInfo{ci}
	r.Annotate(annotated)
	got := annotated[0].Probe
	if got == nil || !got.Configured || !got.Ok || got.FailStreak != 0 {
		t.Fatalf("expected configured=true, ok=true, fail_streak=0; got %+v", got)
	}
	if got.Route != "bridge" {
		t.Fatalf("expected route=bridge, got %q", got.Route)
	}
	if got.LastStatus == nil || *got.LastStatus != 200 {
		t.Fatalf("expected last_status=200, got %v", got.LastStatus)
	}
}

func TestRunner_FailingHTTP_BumpsStreak(t *testing.T) {
	host, port, srv, _ := startTestServer(t, "/health", 503)
	defer srv.Close()

	r := NewRunner(newRouterFromSet("bridge"))
	defer r.Stop()

	ci := makeContainer("idF", "broken_app", host, map[string]string{
		"nanodatacenter.probe.ui.path":          "/health",
		"nanodatacenter.probe.ui.internal_port": strconv.Itoa(port),
		"nanodatacenter.probe.ui.interval_s":    "1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Reconcile(ctx, []dockercli.ContainerInfo{ci})

	// Wait for at least 2 probes to land (initial + one tick).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		annotated := []dockercli.ContainerInfo{ci}
		r.Annotate(annotated)
		if annotated[0].Probe != nil && annotated[0].Probe.FailStreak >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	annotated := []dockercli.ContainerInfo{ci}
	r.Annotate(annotated)
	got := annotated[0].Probe
	if got == nil || got.Ok || got.FailStreak < 2 {
		t.Fatalf("expected ok=false fail_streak>=2; got %+v", got)
	}
	if got.LastStatus == nil || *got.LastStatus != 503 {
		t.Fatalf("expected last_status=503, got %v", got.LastStatus)
	}
}

func TestRunner_NoRoute_NeverProbesNoFailStreak(t *testing.T) {
	// Container is on planner-net only; broker only on bridge.
	r := NewRunner(newRouterFromSet("bridge"))
	defer r.Stop()

	ci := dockercli.ContainerInfo{
		ID:    "idN",
		Name:  "isolated",
		State: "running",
		Labels: map[string]string{
			"nanodatacenter.probe.ui.path":          "/health",
			"nanodatacenter.probe.ui.internal_port": "9000",
		},
		IPAddresses: map[string]string{"planner-net": "10.0.0.5"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Reconcile(ctx, []dockercli.ContainerInfo{ci})

	time.Sleep(200 * time.Millisecond) // give any erroneous goroutine a chance to misbehave

	annotated := []dockercli.ContainerInfo{ci}
	r.Annotate(annotated)
	got := annotated[0].Probe
	if got == nil {
		t.Fatalf("expected non-nil probe state for configured-but-unrouted container")
	}
	if got.Route != RouteNoRoute {
		t.Fatalf("expected route=no_route, got %q", got.Route)
	}
	if got.FailStreak != 0 {
		t.Fatalf("no_route must NOT bump fail_streak; got %d", got.FailStreak)
	}
	if got.LastProbeTs != nil {
		t.Fatalf("no_route must NOT have probed; last_probe_ts should be nil")
	}
}

func TestRunner_RecreatedContainer_NewIDReplacesOld(t *testing.T) {
	host, port, srv, _ := startTestServer(t, "/health", 200)
	defer srv.Close()

	r := NewRunner(newRouterFromSet("bridge"))
	defer r.Stop()

	old := makeContainer("idOLD", "app", host, map[string]string{
		"nanodatacenter.probe.ui.path":          "/health",
		"nanodatacenter.probe.ui.internal_port": strconv.Itoa(port),
		"nanodatacenter.probe.ui.interval_s":    "1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Reconcile(ctx, []dockercli.ContainerInfo{old})

	// Replace with new ID, same name (simulating rm + run).
	fresh := old
	fresh.ID = "idNEW"
	r.Reconcile(ctx, []dockercli.ContainerInfo{fresh})

	r.mu.Lock()
	_, oldStillThere := r.containers["idOLD"]
	_, newThere := r.containers["idNEW"]
	r.mu.Unlock()

	if oldStillThere {
		t.Fatalf("old ID should be cancelled+removed after Reconcile with new ID")
	}
	if !newThere {
		t.Fatalf("new ID should be present after Reconcile")
	}
}

func TestRunner_DroppedContainer_RemovesEntry(t *testing.T) {
	host, port, srv, _ := startTestServer(t, "/health", 200)
	defer srv.Close()

	r := NewRunner(newRouterFromSet("bridge"))
	defer r.Stop()

	ci := makeContainer("idD", "app", host, map[string]string{
		"nanodatacenter.probe.ui.path":          "/health",
		"nanodatacenter.probe.ui.internal_port": strconv.Itoa(port),
		"nanodatacenter.probe.ui.interval_s":    "1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Reconcile(ctx, []dockercli.ContainerInfo{ci})

	r.Reconcile(ctx, []dockercli.ContainerInfo{}) // gone now

	r.mu.Lock()
	n := len(r.containers)
	r.mu.Unlock()

	if n != 0 {
		t.Fatalf("expected runner to be empty after container drop, got %d entries", n)
	}
}

func TestRunner_NotRunning_NoProbe(t *testing.T) {
	r := NewRunner(newRouterFromSet("bridge"))
	defer r.Stop()

	ci := makeContainer("idStopped", "app", "127.0.0.1", map[string]string{
		"nanodatacenter.probe.ui.path":          "/health",
		"nanodatacenter.probe.ui.internal_port": "9999",
	})
	ci.State = "exited"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Reconcile(ctx, []dockercli.ContainerInfo{ci})

	r.mu.Lock()
	n := len(r.containers)
	r.mu.Unlock()

	if n != 0 {
		t.Fatalf("exited container should not get probe goroutines, got %d entries", n)
	}
}

func TestRunner_Annotate_UnknownContainer_NoOp(t *testing.T) {
	r := NewRunner(newRouterFromSet("bridge"))
	defer r.Stop()

	list := []dockercli.ContainerInfo{
		makeContainer("unknown", "x", "127.0.0.1", nil),
	}
	r.Annotate(list)
	if list[0].Probe != nil {
		t.Fatalf("expected nil Probe on unknown container, got %+v", list[0].Probe)
	}
}

// --- helpers ---

// waitForProbeOk polls Annotate until the named container reports ok==expectOk
// or the deadline expires.
func waitForProbeOk(r *Runner, id string, expectOk bool, max time.Duration) bool {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		list := []dockercli.ContainerInfo{{ID: id}}
		r.Annotate(list)
		if p := list[0].Probe; p != nil && p.Ok == expectOk && p.LastProbeTs != nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

