// Package httpapi exposes the broker's HTTP routes per the wire
// protocol: /v1/health, /v1/state/*, /v1/cmd/*. Listens on a TCP
// address; unix-socket support is deferred until a consumer needs it.
//
// The HTTP path is the NATS-down fallback for reads and the canonical
// path for mutations. Per WIRE_PROTOCOL.md, supervisors MUST NOT poll
// state endpoints in their hot path — NATS subscriptions are the
// only sanctioned read path.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
	"github.com/nanodatacenter/docker_host_broker/internal/state"
)

// Server bundles the broker's HTTP listener and its dependencies.
type Server struct {
	httpAddr string
	cache    *state.Cache
	status   *state.Status
	docker   dockercli.Client
	mux      *http.ServeMux
	srv      *http.Server
}

// New constructs a Server with all routes wired.
func New(httpAddr string, cache *state.Cache, status *state.Status, docker dockercli.Client) *Server {
	s := &Server{
		httpAddr: httpAddr,
		cache:    cache,
		status:   status,
		docker:   docker,
		mux:      http.NewServeMux(),
	}
	s.routes()
	s.srv = &http.Server{
		Addr:              httpAddr,
		Handler:           s.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/v1/health", s.handleHealth)
	s.mux.HandleFunc("/v1/state/containers", s.handleStateContainers)
	s.mux.HandleFunc("/v1/state/containers/", s.handleStateContainerByName)
	s.mux.HandleFunc("/v1/state/host_metrics", s.handleStateHostMetrics)
	s.mux.HandleFunc("/v1/cmd/start", s.handleCmdStart)
	s.mux.HandleFunc("/v1/cmd/stop", s.handleCmdStop)
	s.mux.HandleFunc("/v1/cmd/run", s.handleCmdRun)
	s.mux.HandleFunc("/v1/cmd/rm", s.handleCmdRm)
}

// Run starts the listener and blocks until ctx is cancelled or the
// server fails. Triggers a graceful shutdown on context cancel.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		log.Printf("httpapi: listening on %s", s.httpAddr)
		err := s.srv.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("httpapi: shutdown: %w", err)
		}
		return nil
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	}
}

// --- helpers ------------------------------------------------------------------

func tsFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("httpapi: encode: %v", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

// --- handlers -----------------------------------------------------------------

type healthBody struct {
	Status         string  `json:"status"`
	DockerSocketOK bool    `json:"docker_socket_ok"`
	UptimeS        float64 `json:"uptime_s"`
	BrokerVersion  string  `json:"broker_version"`
	HeartbeatSeq   uint64  `json:"heartbeat_seq"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	body := healthBody{
		DockerSocketOK: s.status.DockerSocketOK(),
		UptimeS:        s.status.Uptime().Seconds(),
		BrokerVersion:  s.status.BrokerVersion(),
		HeartbeatSeq:   s.status.HeartbeatSeq(),
	}
	if body.DockerSocketOK {
		body.Status = "healthy"
		writeJSON(w, http.StatusOK, body)
	} else {
		body.Status = "degraded"
		writeJSON(w, http.StatusServiceUnavailable, body)
	}
}

type stateContainersBody struct {
	TS         float64                   `json:"ts"`
	Seq        uint64                    `json:"seq"`
	Containers []dockercli.ContainerInfo `json:"containers"`
}

func (s *Server) handleStateContainers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	ts, seq, list := s.cache.SnapshotContainers()
	writeJSON(w, http.StatusOK, stateContainersBody{
		TS:         tsFloat(ts),
		Seq:        seq,
		Containers: list,
	})
}

type stateContainerBody struct {
	TS        float64                  `json:"ts"`
	Seq       uint64                   `json:"seq"`
	Container dockercli.ContainerInfo  `json:"container"`
}

func (s *Server) handleStateContainerByName(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/v1/state/containers/")
	if name == "" || strings.Contains(name, "/") {
		writeError(w, http.StatusBadRequest, "missing or invalid container name")
		return
	}
	ts, seq, ci, ok := s.cache.SnapshotContainer(name)
	if !ok {
		writeError(w, http.StatusNotFound, "container not found")
		return
	}
	writeJSON(w, http.StatusOK, stateContainerBody{
		TS:        tsFloat(ts),
		Seq:       seq,
		Container: ci,
	})
}

type stateHostMetricsBody struct {
	TS   float64           `json:"ts"`
	Seq  uint64            `json:"seq"`
	Host state.HostMetrics `json:"host"`
}

func (s *Server) handleStateHostMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	ts, seq, hm := s.cache.SnapshotHostMetrics()
	writeJSON(w, http.StatusOK, stateHostMetricsBody{
		TS:   tsFloat(ts),
		Seq:  seq,
		Host: hm,
	})
}

// --- Mutation handlers --------------------------------------------------------
//
// All four endpoints share a small contract:
//   * POST only; anything else → 405.
//   * Body must decode cleanly; bad JSON / missing required fields → 400.
//   * Each successful submission gets a fresh cmd_id (RFC4122 v4) so
//     callers can correlate request → containers.delta event in NATS.
//   * dockercli sentinels map onto wire-protocol status codes:
//        ErrNotFound        → 404
//        ErrAlreadyRunning  → 409 {"error":"already_running"}
//        ErrAlreadyStopped  → 409 {"error":"already_stopped"}
//        *NameTakenError    → 409 {"error":"name_taken","existing_id":...}
//        ErrRunningConflict → 409 {"error":"running_force_required"}
//
// Per WIRE_PROTOCOL.md the supervisor verifies success via the next
// containers.snapshot, not via this response — so we don't block here
// waiting for the daemon to settle.

func newCmdID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is catastrophic; fall back to ts so we
		// at least return a cmd_id rather than empty string.
		return fmt.Sprintf("ts-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func decodePOST(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST only")
		return false
	}
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}

type cmdStartRequest struct {
	Name string `json:"name"`
}

type cmdAccepted struct {
	Name       string  `json:"name"`
	ID         string  `json:"id,omitempty"`
	CmdID      string  `json:"cmd_id"`
	AcceptedAt float64 `json:"accepted_at"`
}

func (s *Server) handleCmdStart(w http.ResponseWriter, r *http.Request) {
	var req cmdStartRequest
	if !decodePOST(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	err := s.docker.StartContainer(r.Context(), req.Name)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, cmdAccepted{
			Name: req.Name, CmdID: newCmdID(), AcceptedAt: tsFloat(time.Now()),
		})
	case errors.Is(err, dockercli.ErrNotFound):
		writeError(w, http.StatusNotFound, "container not found")
	case errors.Is(err, dockercli.ErrAlreadyRunning):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "already_running", "name": req.Name,
		})
	default:
		log.Printf("httpapi: start(%s): %v", req.Name, err)
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

type cmdStopRequest struct {
	Name     string `json:"name"`
	TimeoutS int    `json:"timeout_s"`
}

func (s *Server) handleCmdStop(w http.ResponseWriter, r *http.Request) {
	var req cmdStopRequest
	if !decodePOST(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	timeout := time.Duration(req.TimeoutS) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second // wire-protocol default
	}
	err := s.docker.StopContainer(r.Context(), req.Name, timeout)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, cmdAccepted{
			Name: req.Name, CmdID: newCmdID(), AcceptedAt: tsFloat(time.Now()),
		})
	case errors.Is(err, dockercli.ErrNotFound):
		writeError(w, http.StatusNotFound, "container not found")
	case errors.Is(err, dockercli.ErrAlreadyStopped):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "already_stopped", "name": req.Name,
		})
	default:
		log.Printf("httpapi: stop(%s): %v", req.Name, err)
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

type cmdRunRequest struct {
	Name          string            `json:"name"`
	Image         string            `json:"image"`
	Env           map[string]string `json:"env"`
	Ports         []cmdRunPort      `json:"ports"`
	Volumes       []cmdRunVolume    `json:"volumes"`
	RestartPolicy string            `json:"restart_policy"`
	Labels        map[string]string `json:"labels"`
	Network       string            `json:"network"`
	ExtraHosts    []string          `json:"extra_hosts"`
	Entrypoint    []string          `json:"entrypoint"`
}

type cmdRunPort struct {
	HostIP        string `json:"host_ip"`
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Proto         string `json:"proto"`
}

type cmdRunVolume struct {
	Host      string `json:"host"`
	Container string `json:"container"`
	ReadOnly  bool   `json:"read_only"`
}

func (s *Server) handleCmdRun(w http.ResponseWriter, r *http.Request) {
	var req cmdRunRequest
	if !decodePOST(w, r, &req) {
		return
	}
	if req.Name == "" || req.Image == "" {
		writeError(w, http.StatusBadRequest, "name and image are required")
		return
	}

	spec := dockercli.RunSpec{
		Name:          req.Name,
		Image:         req.Image,
		Env:           req.Env,
		RestartPolicy: req.RestartPolicy,
		Labels:        req.Labels,
		Network:       req.Network,
		ExtraHosts:    req.ExtraHosts,
		Entrypoint:    req.Entrypoint,
	}
	for _, p := range req.Ports {
		spec.Ports = append(spec.Ports, dockercli.PortBinding{
			HostIP: p.HostIP, HostPort: p.HostPort,
			ContainerPort: p.ContainerPort, Proto: p.Proto,
		})
	}
	for _, v := range req.Volumes {
		spec.Volumes = append(spec.Volumes, dockercli.VolumeBinding{
			HostPath: v.Host, ContainerPath: v.Container, ReadOnly: v.ReadOnly,
		})
	}

	id, err := s.docker.RunContainer(r.Context(), spec)
	if err == nil {
		writeJSON(w, http.StatusAccepted, cmdAccepted{
			Name: req.Name, ID: id, CmdID: newCmdID(), AcceptedAt: tsFloat(time.Now()),
		})
		return
	}

	var nameTaken *dockercli.NameTakenError
	if errors.As(err, &nameTaken) {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "name_taken", "name": nameTaken.Name, "existing_id": nameTaken.ExistingID,
		})
		return
	}

	log.Printf("httpapi: run(%s): %v", req.Name, err)
	writeError(w, http.StatusInternalServerError, err.Error())
}

type cmdRmRequest struct {
	Name  string `json:"name"`
	Force bool   `json:"force"`
}

func (s *Server) handleCmdRm(w http.ResponseWriter, r *http.Request) {
	var req cmdRmRequest
	if !decodePOST(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	err := s.docker.RemoveContainer(r.Context(), req.Name, req.Force)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, cmdAccepted{
			Name: req.Name, CmdID: newCmdID(), AcceptedAt: tsFloat(time.Now()),
		})
	case errors.Is(err, dockercli.ErrNotFound):
		// Wire protocol: 404 is idempotent — chain-tree treats as success.
		writeError(w, http.StatusNotFound, "container not found")
	case errors.Is(err, dockercli.ErrRunningConflict):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error": "running_force_required", "name": req.Name,
		})
	default:
		log.Printf("httpapi: rm(%s): %v", req.Name, err)
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}
