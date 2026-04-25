package probes

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
)

// Runner owns the per-(container × slot) probe goroutines and their
// state. It is keyed by container ID — when a container is rm+run'd
// the new ID gets fresh goroutines and the old ones are cancelled.
//
// Lifecycle:
//   r := NewRunner(router)
//   defer r.Stop()
//   // every container poll cycle (5s):
//   r.Reconcile(containerList)
//   // at snapshot publish time:
//   r.Annotate(snap)  // fills snap[i].Probe
type Runner struct {
	router *Router

	mu sync.Mutex
	// containers keyed by ID. The key set is the runner's authoritative
	// view of "which containers should currently have probe goroutines".
	containers map[string]*containerState
}

type containerState struct {
	id     string
	name   string
	cancel context.CancelFunc

	// per-slot state, mu must be held to read/write.
	slots map[string]*slotState
}

// slotState holds the running state of one (container × slot) probe.
// Updated only by the slot's goroutine; read by Annotate under
// containerState's parent Runner.mu.
type slotState struct {
	spec  SlotSpec
	route string // resolved at goroutine start; "no_route" disables probing

	ok          bool
	failStreak  int
	lastOkTs    *float64
	lastProbeTs *float64
	lastStatus  *int
	lastErr     *string
}

// NewRunner returns a Runner ready to Reconcile.
func NewRunner(router *Router) *Runner {
	return &Runner{
		router:     router,
		containers: map[string]*containerState{},
	}
}

// Reconcile reads the current container list and reshapes the runner's
// goroutines to match: starts goroutines for new probed containers,
// cancels goroutines for containers no longer in the list. Containers
// without any nanodatacenter.probe.* labels are ignored.
//
// Should be called once per container-poll cycle on the same goroutine
// that updates the cache; not safe for concurrent Reconcile calls but
// safe to overlap with Annotate.
func (r *Runner) Reconcile(parentCtx context.Context, list []dockercli.ContainerInfo) {
	want := map[string]dockercli.ContainerInfo{}
	for _, ci := range list {
		if ci.ID == "" || ci.State != "running" {
			continue
		}
		slots, errs := ParseLabels(ci.Labels)
		for _, e := range errs {
			log.Printf("probes: %s: %v", ci.Name, e)
		}
		if len(slots) == 0 {
			continue
		}
		want[ci.ID] = ci
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Cancel containers we no longer want (ID not in `want`).
	for id, cs := range r.containers {
		if _, keep := want[id]; !keep {
			cs.cancel()
			delete(r.containers, id)
		}
	}

	// Spawn goroutines for new IDs. If a container is rm+run'd the new
	// ID won't match the old one in our map, so we'll naturally start
	// fresh; the old IDs were already cancelled by the loop above.
	for id, ci := range want {
		if _, exists := r.containers[id]; exists {
			continue
		}
		slots, _ := ParseLabels(ci.Labels) // re-parse; errs already logged above
		ip, route := r.router.PickIP(ci)
		ctx, cancel := context.WithCancel(parentCtx)
		cs := &containerState{
			id:     id,
			name:   ci.Name,
			cancel: cancel,
			slots:  make(map[string]*slotState, len(slots)),
		}
		for _, s := range slots {
			cs.slots[s.Slot] = &slotState{spec: s, route: route}
		}
		r.containers[id] = cs

		if route == RouteNoRoute {
			log.Printf("probes: %s id=%s: no shared network with broker; will report no_route only", ci.Name, shortID(id))
			continue
		}

		log.Printf("probes: %s id=%s route=%s slots=%d", ci.Name, shortID(id), route, len(slots))
		for _, slot := range slots {
			go r.run(ctx, cs, slot, ip)
		}
	}
}

// run is one (container × slot) probe loop. Exits when ctx is done.
func (r *Runner) run(ctx context.Context, cs *containerState, spec SlotSpec, ip string) {
	url := fmt.Sprintf("http://%s:%d%s", ip, spec.InternalPort, spec.Path)
	client := &http.Client{Timeout: spec.Timeout}

	probe := func() {
		now := nowFloat()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			r.recordFailure(cs, spec.Slot, now, nil, fmt.Sprintf("request build: %v", err))
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			// Distinguish ctx cancellation from real probe failures.
			if ctx.Err() != nil {
				return
			}
			r.recordFailure(cs, spec.Slot, now, nil, shortHttpErr(err))
			return
		}
		_ = resp.Body.Close()
		status := resp.StatusCode
		if status == spec.ExpectStatus {
			r.recordSuccess(cs, spec.Slot, now, status)
			return
		}
		r.recordFailure(cs, spec.Slot, now, &status, fmt.Sprintf("http %d", status))
	}

	probe()
	t := time.NewTicker(spec.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			probe()
		}
	}
}

func (r *Runner) recordSuccess(cs *containerState, slot string, now float64, status int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := cs.slots[slot]
	if st == nil {
		return
	}
	st.ok = true
	st.failStreak = 0
	ok := now
	st.lastOkTs = &ok
	probe := now
	st.lastProbeTs = &probe
	s := status
	st.lastStatus = &s
	st.lastErr = nil
}

func (r *Runner) recordFailure(cs *containerState, slot string, now float64, status *int, errStr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := cs.slots[slot]
	if st == nil {
		return
	}
	st.ok = false
	st.failStreak++
	probe := now
	st.lastProbeTs = &probe
	st.lastStatus = status
	e := errStr
	st.lastErr = &e
}

// Annotate fills in the Probe field on each ContainerInfo whose ID is
// known to the runner. Called by the snapshot publisher right before
// pushing to NATS / pg / HTTP.
func (r *Runner) Annotate(list []dockercli.ContainerInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range list {
		cs, ok := r.containers[list[i].ID]
		if !ok {
			continue // no probes configured on this container
		}
		list[i].Probe = aggregateLocked(cs)
	}
}

// aggregateLocked computes the cross-slot ProbeState. Caller must hold r.mu.
//
// Aggregation rule (per WIRE_PROTOCOL.md § Phase 4): Ok = AND across
// slots, FailStreak = max, LastErr from the worst slot. Per-slot
// detail not in the snapshot — exposed only via /v1/state/containers/<name>.
func aggregateLocked(cs *containerState) *dockercli.ProbeState {
	if len(cs.slots) == 0 {
		return nil
	}

	// Common case: every slot reports route=no_route (broker has no
	// path to the container). Surface this as a single no_route entry
	// and freeze ok at false-but-not-failing.
	allNoRoute := true
	for _, st := range cs.slots {
		if st.route != RouteNoRoute {
			allNoRoute = false
			break
		}
	}
	if allNoRoute {
		return &dockercli.ProbeState{
			Configured: true,
			Ok:         false,
			Route:      RouteNoRoute,
		}
	}

	out := &dockercli.ProbeState{
		Configured: true,
		Ok:         true,
		Route:      "", // overwritten below
	}
	// Pick a route to report. All routable slots on a container share
	// the same network (resolved once at Reconcile time), so any non-
	// no_route slot's route is representative.
	for _, st := range cs.slots {
		if st.route != RouteNoRoute {
			out.Route = st.route
			break
		}
	}

	var (
		worstSlot      *slotState // highest fail_streak — sources LastErr on failure
		mostRecentSlot *slotState // latest lastProbeTs — sources LastStatus
		anyProbed      bool
	)
	for _, st := range cs.slots {
		if st.lastProbeTs != nil {
			anyProbed = true
		}
		if !st.ok {
			out.Ok = false
		}
		if worstSlot == nil || st.failStreak > worstSlot.failStreak {
			worstSlot = st
		}
		if st.lastProbeTs != nil &&
			(mostRecentSlot == nil || mostRecentSlot.lastProbeTs == nil ||
				*st.lastProbeTs > *mostRecentSlot.lastProbeTs) {
			mostRecentSlot = st
		}
		if st.failStreak > out.FailStreak {
			out.FailStreak = st.failStreak
		}
		if st.lastOkTs != nil && (out.LastOkTs == nil || *st.lastOkTs > *out.LastOkTs) {
			out.LastOkTs = st.lastOkTs
		}
		if st.lastProbeTs != nil && (out.LastProbeTs == nil || *st.lastProbeTs > *out.LastProbeTs) {
			out.LastProbeTs = st.lastProbeTs
		}
	}

	// Until any probe has actually run, report Ok=false to avoid
	// claiming success before evidence exists. Supervisors that gate
	// on freshness (LastProbeTs == nil) will already skip; this guard
	// is belt-and-suspenders.
	if !anyProbed {
		out.Ok = false
	}

	// last_status reflects the most recent attempt regardless of ok-ness.
	// last_err is set only when Ok=false (per wire-protocol contract).
	if mostRecentSlot != nil {
		out.LastStatus = mostRecentSlot.lastStatus
	}
	if !out.Ok && worstSlot != nil {
		out.LastErr = worstSlot.lastErr
	}

	return out
}

// Stop cancels every running probe goroutine. Safe to call multiple times.
func (r *Runner) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, cs := range r.containers {
		cs.cancel()
		delete(r.containers, id)
	}
}

func nowFloat() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// shortHttpErr trims the noisy `Get "http://...": ` prefix that net/http
// adds, leaving just the underlying transport error like "connect refused"
// or "context deadline exceeded".
func shortHttpErr(err error) string {
	s := err.Error()
	const sep = `": `
	if i := strings.Index(s, sep); i >= 0 {
		return s[i+len(sep):]
	}
	return s
}
