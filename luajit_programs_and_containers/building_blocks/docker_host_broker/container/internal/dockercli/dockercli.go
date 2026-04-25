// Package dockercli wraps the Docker SDK client into the small set of
// operations the broker needs. All access to the Docker daemon goes
// through this package — no other package may import the Docker SDK
// directly. This boundary lets us unit-test the rest of the broker
// against a fake client.
package dockercli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
)

// Client is the broker's interface to the Docker daemon. The concrete
// implementation in this package wraps github.com/docker/docker/client;
// tests can substitute an in-memory fake.
type Client interface {
	ListContainers(ctx context.Context) ([]ContainerInfo, error)
	InspectContainer(ctx context.Context, name string) (ContainerInfo, error)
	Stats(ctx context.Context, name string) (RawStats, error)
	StartContainer(ctx context.Context, name string) error
	StopContainer(ctx context.Context, name string, timeout time.Duration) error
	RunContainer(ctx context.Context, spec RunSpec) (string, error)
	RemoveContainer(ctx context.Context, name string, force bool) error
	Ping(ctx context.Context) error
	Close() error
}

// ContainerInfo is the broker's view of a container, mapped onto the
// wire-protocol schema (see WIRE_PROTOCOL.md § containers.snapshot).
// JSON tags make the marshal output match the wire schema directly.
type ContainerInfo struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Image       string            `json:"image"`
	State       string            `json:"state"`
	StartedAt   float64           `json:"started_at"`
	FinishedAt  float64           `json:"finished_at"`
	ExitCode    *int              `json:"exit_code"`
	Health      string            `json:"health"`
	Ports       []PortBinding     `json:"ports"`
	Labels      map[string]string `json:"labels"`
	IPAddresses map[string]string `json:"ip_addresses"`
	// Probe is the broker's active-HTTP-probe state, aggregated across
	// all probed slots on the container. nil (serializes as JSON null)
	// for containers with no nanodatacenter.probe.* labels. See
	// WIRE_PROTOCOL.md § "Broker-active HTTP probes (Phase 4)".
	Probe *ProbeState `json:"probe"`
}

// ProbeState mirrors the wire-protocol probe sub-object. All fields are
// the cross-slot aggregate when a container has multiple probed slots:
// Ok = AND across slots; FailStreak = max; LastErr from the worst slot.
// Per-slot detail is exposed via /v1/state/containers/<name> for
// debugging, not in the snapshot.
type ProbeState struct {
	Configured  bool     `json:"configured"`
	Ok          bool     `json:"ok"`
	FailStreak  int      `json:"fail_streak"`
	LastOkTs    *float64 `json:"last_ok_ts"`
	LastProbeTs *float64 `json:"last_probe_ts"`
	LastStatus  *int     `json:"last_status"`
	LastErr     *string  `json:"last_err"`
	// Route is the network the probe IP came from, or "no_route" when
	// the broker shares no network with the container. When "no_route"
	// the broker does NOT probe; supervisors MUST treat as skip, not fail.
	Route string `json:"route"`
}

// PortBinding mirrors the wire-protocol port shape.
type PortBinding struct {
	HostIP        string `json:"host_ip"`
	HostPort      int    `json:"host_port"`
	ContainerPort int    `json:"container_port"`
	Proto         string `json:"proto"`
}

// RawStats holds raw cumulative counters from a single docker stats
// call. The state cache uses two consecutive samples to compute rates.
type RawStats struct {
	SampleAt    time.Time
	CPUUsage    uint64 // total CPU nanoseconds
	SystemUsage uint64 // system CPU nanoseconds
	NumCPUs     int
	MemUsage    uint64
	MemLimit    uint64
	BlkRead     uint64
	BlkWrite    uint64
	NetRx       uint64
	NetTx       uint64
}

// RunSpec is the broker's input to RunContainer.
type RunSpec struct {
	Name          string
	Image         string
	Env           map[string]string
	Ports         []PortBinding
	Volumes       []VolumeBinding
	RestartPolicy string
	Labels        map[string]string
	Network       string
	// ExtraHosts maps onto docker's --add-host (slice of "name:ip"
	// strings). DCS uses "host.docker.internal:host-gateway" so app
	// containers can reach pg/nats on the docker host.
	ExtraHosts []string
	// Entrypoint overrides the image's CMD (positional argv); empty
	// means use the image default.
	Entrypoint []string
}

// VolumeBinding mirrors the wire-protocol volume shape.
type VolumeBinding struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
}

// ErrNotImplemented is returned by mutation methods that haven't been
// wired up yet. Kept as a named export so callers can detect it.
var ErrNotImplemented = errors.New("dockercli: not implemented")

// Sentinel errors for the mutation API. httpapi maps these onto the
// wire-protocol status codes (404 / 409). Consumers that need richer
// detail (e.g., the existing ID for a name_taken response) use
// errors.As against the typed variants below.
var (
	ErrNotFound        = errors.New("dockercli: container not found")
	ErrAlreadyRunning  = errors.New("dockercli: container already running")
	ErrAlreadyStopped  = errors.New("dockercli: container already stopped")
	ErrRunningConflict = errors.New("dockercli: container is running (force=false)")
)

// NameTakenError carries the existing container's ID so httpapi can
// echo it back per WIRE_PROTOCOL.md § /v1/cmd/run.
type NameTakenError struct {
	Name       string
	ExistingID string
}

func (e *NameTakenError) Error() string {
	return fmt.Sprintf("dockercli: name_taken: %s (existing_id=%s)", e.Name, e.ExistingID)
}

// realClient is the production Docker SDK-backed Client.
type realClient struct {
	cli *client.Client
}

// New constructs the production Client. dockerHost is the daemon URL
// (e.g., "unix:///var/run/docker.sock"); pass "" to use the env
// (DOCKER_HOST) or the default unix socket.
func New(ctx context.Context, dockerHost string) (Client, error) {
	opts := []client.Opt{client.WithAPIVersionNegotiation()}
	if dockerHost != "" {
		opts = append(opts, client.WithHost(dockerHost))
	} else {
		opts = append(opts, client.FromEnv)
	}
	cli, err := client.NewClientWithOpts(opts...)
	if err != nil {
		return nil, fmt.Errorf("dockercli: NewClientWithOpts: %w", err)
	}
	if _, err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("dockercli: ping: %w", err)
	}
	return &realClient{cli: cli}, nil
}

func (c *realClient) Close() error {
	return c.cli.Close()
}

func (c *realClient) Ping(ctx context.Context) error {
	_, err := c.cli.Ping(ctx)
	return err
}

// trimName drops the leading "/" docker prepends to container names.
func trimName(n string) string {
	return strings.TrimPrefix(n, "/")
}

// pickPrimaryName returns the first non-empty trimmed name, or "" if none.
func pickPrimaryName(names []string) string {
	for _, n := range names {
		if t := trimName(n); t != "" {
			return t
		}
	}
	return ""
}

// parseDockerTime converts an RFC3339 timestamp from inspect output
// to unix seconds. Returns 0 on parse failure or "0001-01-01T00:00:00Z"
// (docker's "never" sentinel).
func parseDockerTime(s string) float64 {
	if s == "" || strings.HasPrefix(s, "0001-01-01") {
		return 0
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return 0
		}
	}
	return float64(t.UnixNano()) / 1e9
}

func (c *realClient) ListContainers(ctx context.Context) ([]ContainerInfo, error) {
	list, err := c.cli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("dockercli: ContainerList: %w", err)
	}
	out := make([]ContainerInfo, 0, len(list))
	for _, c := range list {
		ci := ContainerInfo{
			ID:        c.ID,
			Name:      pickPrimaryName(c.Names),
			Image:     c.Image,
			State:     c.State,
			Labels:    map[string]string{},
			Ports:     make([]PortBinding, 0, len(c.Ports)),
			Health:    "none",
			IPAddresses: map[string]string{},
		}
		// ContainerList's State doesn't include exit code; leave nil here.
		// Caller can call InspectContainer for the full per-container detail.
		for k, v := range c.Labels {
			ci.Labels[k] = v
		}
		for _, p := range c.Ports {
			ci.Ports = append(ci.Ports, PortBinding{
				HostIP:        p.IP,
				HostPort:      int(p.PublicPort),
				ContainerPort: int(p.PrivatePort),
				Proto:         strings.ToLower(p.Type),
			})
		}
		if c.NetworkSettings != nil {
			for net, ep := range c.NetworkSettings.Networks {
				if ep != nil && ep.IPAddress != "" {
					ci.IPAddresses[net] = ep.IPAddress
				}
			}
		}
		out = append(out, ci)
	}
	return out, nil
}

func (c *realClient) InspectContainer(ctx context.Context, name string) (ContainerInfo, error) {
	j, err := c.cli.ContainerInspect(ctx, name)
	if err != nil {
		return ContainerInfo{}, fmt.Errorf("dockercli: ContainerInspect(%s): %w", name, err)
	}
	ci := ContainerInfo{
		ID:          j.ID,
		Name:        trimName(j.Name),
		Image:       j.Config.Image,
		Labels:      map[string]string{},
		Ports:       []PortBinding{},
		IPAddresses: map[string]string{},
		Health:      "none",
	}
	for k, v := range j.Config.Labels {
		ci.Labels[k] = v
	}
	if j.State != nil {
		ci.State = j.State.Status
		ci.StartedAt = parseDockerTime(j.State.StartedAt)
		ci.FinishedAt = parseDockerTime(j.State.FinishedAt)
		if ci.State == "exited" || ci.State == "dead" {
			ec := j.State.ExitCode
			ci.ExitCode = &ec
		}
		if j.State.Health != nil {
			ci.Health = j.State.Health.Status
		}
	}
	if j.NetworkSettings != nil {
		for portProto, bindings := range j.NetworkSettings.Ports {
			cport := portProto.Int()
			proto := strings.ToLower(portProto.Proto())
			for _, b := range bindings {
				port := 0
				if b.HostPort != "" {
					_, _ = fmt.Sscanf(b.HostPort, "%d", &port)
				}
				ci.Ports = append(ci.Ports, PortBinding{
					HostIP:        b.HostIP,
					HostPort:      port,
					ContainerPort: cport,
					Proto:         proto,
				})
			}
		}
		for net, ep := range j.NetworkSettings.Networks {
			if ep != nil && ep.IPAddress != "" {
				ci.IPAddresses[net] = ep.IPAddress
			}
		}
	}
	return ci, nil
}

// Stats issues a one-shot docker stats call and returns the raw
// cumulative counters. Rate computation lives in package state.
func (c *realClient) Stats(ctx context.Context, name string) (RawStats, error) {
	resp, err := c.cli.ContainerStatsOneShot(ctx, name)
	if err != nil {
		return RawStats{}, fmt.Errorf("dockercli: ContainerStatsOneShot(%s): %w", name, err)
	}
	defer resp.Body.Close()

	var s dockertypes.StatsJSON
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return RawStats{}, fmt.Errorf("dockercli: decode stats(%s): %w", name, err)
	}

	var blkRead, blkWrite uint64
	for _, e := range s.BlkioStats.IoServiceBytesRecursive {
		switch strings.ToLower(e.Op) {
		case "read":
			blkRead += e.Value
		case "write":
			blkWrite += e.Value
		}
	}

	var netRx, netTx uint64
	for _, n := range s.Networks {
		netRx += n.RxBytes
		netTx += n.TxBytes
	}

	numCPUs := int(s.CPUStats.OnlineCPUs)
	if numCPUs == 0 {
		numCPUs = len(s.CPUStats.CPUUsage.PercpuUsage)
	}

	return RawStats{
		SampleAt:    s.Read,
		CPUUsage:    s.CPUStats.CPUUsage.TotalUsage,
		SystemUsage: s.CPUStats.SystemUsage,
		NumCPUs:     numCPUs,
		MemUsage:    s.MemoryStats.Usage,
		MemLimit:    s.MemoryStats.Limit,
		BlkRead:     blkRead,
		BlkWrite:    blkWrite,
		NetRx:       netRx,
		NetTx:       netTx,
	}, nil
}

// --- Mutation API ------------------------------------------------------------
//
// All four methods pre-check current state via inspect and surface
// idempotency outcomes as typed sentinels (ErrAlreadyRunning, etc.).
// The pre-check is one extra daemon roundtrip; mutations are rare
// (boot, teardown, occasional respawn) so the cost is irrelevant
// compared to the clean error contract for the HTTP layer.

func isNotFound(err error) bool {
	return err != nil && (errdefs.IsNotFound(err) || client.IsErrNotFound(err))
}

func (c *realClient) StartContainer(ctx context.Context, name string) error {
	j, err := c.cli.ContainerInspect(ctx, name)
	if isNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("dockercli: StartContainer inspect(%s): %w", name, err)
	}
	if j.State != nil && j.State.Running {
		return ErrAlreadyRunning
	}
	if err := c.cli.ContainerStart(ctx, name, container.StartOptions{}); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("dockercli: ContainerStart(%s): %w", name, err)
	}
	return nil
}

func (c *realClient) StopContainer(ctx context.Context, name string, timeout time.Duration) error {
	j, err := c.cli.ContainerInspect(ctx, name)
	if isNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("dockercli: StopContainer inspect(%s): %w", name, err)
	}
	if j.State == nil || !j.State.Running {
		return ErrAlreadyStopped
	}
	opts := container.StopOptions{}
	if timeout > 0 {
		secs := int(timeout / time.Second)
		opts.Timeout = &secs
	}
	if err := c.cli.ContainerStop(ctx, name, opts); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("dockercli: ContainerStop(%s): %w", name, err)
	}
	return nil
}

// RunContainer creates and starts a new container. Returns the new
// container ID on success. Returns *NameTakenError if a container
// with the same name already exists (caller must rm first).
func (c *realClient) RunContainer(ctx context.Context, spec RunSpec) (string, error) {
	if spec.Name == "" {
		return "", fmt.Errorf("dockercli: RunContainer: name is required")
	}
	if spec.Image == "" {
		return "", fmt.Errorf("dockercli: RunContainer: image is required")
	}

	// Pre-check: name collision yields 409 with existing ID.
	if existing, err := c.cli.ContainerInspect(ctx, spec.Name); err == nil {
		return "", &NameTakenError{Name: spec.Name, ExistingID: existing.ID}
	} else if !isNotFound(err) {
		return "", fmt.Errorf("dockercli: RunContainer pre-inspect(%s): %w", spec.Name, err)
	}

	cfg, hostCfg, netCfg, err := buildCreateConfigs(spec)
	if err != nil {
		return "", err
	}
	resp, err := c.cli.ContainerCreate(ctx, cfg, hostCfg, netCfg, nil, spec.Name)
	if err != nil {
		return "", fmt.Errorf("dockercli: ContainerCreate(%s): %w", spec.Name, err)
	}
	if err := c.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		// Best-effort cleanup of the half-created container so retries
		// see a clean slate rather than a stuck name_taken.
		_ = c.cli.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("dockercli: ContainerStart(%s): %w", spec.Name, err)
	}
	return resp.ID, nil
}

func (c *realClient) RemoveContainer(ctx context.Context, name string, force bool) error {
	j, err := c.cli.ContainerInspect(ctx, name)
	if isNotFound(err) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("dockercli: RemoveContainer inspect(%s): %w", name, err)
	}
	if j.State != nil && j.State.Running && !force {
		return ErrRunningConflict
	}
	if err := c.cli.ContainerRemove(ctx, name, container.RemoveOptions{Force: force}); err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("dockercli: ContainerRemove(%s): %w", name, err)
	}
	return nil
}

// buildCreateConfigs translates a RunSpec into the three Docker SDK
// config structs. Edge cases the supervisor is known to need:
//
//   - env ordering is deterministic (sorted) so retries hash-equivalent
//   - tcp/udp split: spec.Ports[].Proto defaults to tcp if empty
//   - bind mounts use Binds[]; bind-propagation/ro/rw flag honored
//   - non-default network: set EndpointsConfig AND NetworkMode so
//     docker doesn't try to attach the default bridge first
func buildCreateConfigs(spec RunSpec) (*container.Config, *container.HostConfig, *network.NetworkingConfig, error) {
	cfg := &container.Config{
		Image:  spec.Image,
		Labels: map[string]string{},
	}
	for k, v := range spec.Labels {
		cfg.Labels[k] = v
	}

	// Env: deterministic order.
	envKeys := make([]string, 0, len(spec.Env))
	for k := range spec.Env {
		envKeys = append(envKeys, k)
	}
	sortStrings(envKeys)
	cfg.Env = make([]string, 0, len(envKeys))
	for _, k := range envKeys {
		cfg.Env = append(cfg.Env, k+"="+spec.Env[k])
	}

	// Port bindings: ExposedPorts on Config + PortBindings on HostConfig.
	exposed := nat.PortSet{}
	bindings := nat.PortMap{}
	for _, p := range spec.Ports {
		proto := strings.ToLower(p.Proto)
		if proto == "" {
			proto = "tcp"
		}
		cport, err := nat.NewPort(proto, strconv.Itoa(p.ContainerPort))
		if err != nil {
			return nil, nil, nil, fmt.Errorf("dockercli: bad port %d/%s: %w", p.ContainerPort, proto, err)
		}
		exposed[cport] = struct{}{}
		hp := strconv.Itoa(p.HostPort)
		bindings[cport] = append(bindings[cport], nat.PortBinding{
			HostIP:   p.HostIP,
			HostPort: hp,
		})
	}
	cfg.ExposedPorts = exposed

	// Volume bindings: docker SDK's Binds is the simplest path for
	// host→container mounts. Format: "host:container[:ro]".
	binds := make([]string, 0, len(spec.Volumes))
	for _, v := range spec.Volumes {
		bind := v.HostPath + ":" + v.ContainerPath
		if v.ReadOnly {
			bind += ":ro"
		}
		binds = append(binds, bind)
	}

	hostCfg := &container.HostConfig{
		Binds:        binds,
		PortBindings: bindings,
		ExtraHosts:   append([]string{}, spec.ExtraHosts...),
	}
	if spec.RestartPolicy != "" {
		hostCfg.RestartPolicy = container.RestartPolicy{
			Name: container.RestartPolicyMode(spec.RestartPolicy),
		}
	}
	if len(spec.Entrypoint) > 0 {
		// Note: assigning to Cmd (not Entrypoint) preserves the image's
		// ENTRYPOINT and overrides only the default CMD, matching the
		// `docker run image arg1 arg2` behavior the existing
		// docker.run_from_spec relies on.
		cfg.Cmd = append([]string{}, spec.Entrypoint...)
	}

	var netCfg *network.NetworkingConfig
	if spec.Network != "" {
		hostCfg.NetworkMode = container.NetworkMode(spec.Network)
		// Custom user-defined networks: also set EndpointsConfig so the
		// container is attached at create-time, not after a default-bridge
		// detour.
		if spec.Network != "bridge" && spec.Network != "host" && spec.Network != "none" {
			netCfg = &network.NetworkingConfig{
				EndpointsConfig: map[string]*network.EndpointSettings{
					spec.Network: {},
				},
			}
		}
	}

	return cfg, hostCfg, netCfg, nil
}

// sortStrings sorts in place; tiny helper to avoid importing "sort"
// just for one call site (keeps the dockercli import set readable).
func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}
