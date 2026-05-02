// Package natspub publishes broker state to NATS subjects per the
// wire protocol. nats.go's default Connect() options provide the
// reconnect-with-backoff semantics; we don't need to hand-roll that.
//
// Publishes are fire-and-forget on NATS core (no QoS). When the
// connection is down, nats.go buffers up to ReconnectBufSize bytes
// and replays on reconnect. Beyond that, publishes return an error
// which we log and drop — the next snapshot tick will publish fresh
// data anyway, so there's no value in trying to reliable-deliver a
// stale snapshot.
package natspub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
	"github.com/nanodatacenter/docker_host_broker/internal/pathkb"
	"github.com/nanodatacenter/docker_host_broker/internal/state"
)

// Publisher owns the NATS connection and emits the broker's six
// subjects (see WIRE_PROTOCOL.md).
type Publisher interface {
	PublishHeartbeat(ts time.Time, seq uint64, dockerOK bool, uptime time.Duration, version string) error
	PublishContainersSnapshot(ts time.Time, seq uint64, containers []dockercli.ContainerInfo) error
	PublishContainersDelta(ts time.Time, seq uint64, events []state.DeltaEvent) error
	PublishContainersStats(ts time.Time, seq uint64, stats map[string]state.Stats) error
	PublishHostMetrics(ts time.Time, seq uint64, host state.HostMetrics) error
	Close() error
}

type subjects struct {
	heartbeat       string
	snapshot        string
	delta           string
	stats           string
	hostMetrics     string
}

func subjectsFor(site string) subjects {
	root := pathkb.BrokerRoot(site)
	return subjects{
		heartbeat:   root + "heartbeat",
		snapshot:    root + "containers.snapshot",
		delta:       root + "containers.delta",
		stats:       root + "containers.stats",
		hostMetrics: root + "host_metrics.snapshot",
	}
}

type realPublisher struct {
	nc       *nats.Conn
	subjects subjects
}

// Connect dials NATS and returns a Publisher whose subjects are
// rooted at `system.<sys>.site.<site>.docker_broker.*`. natsURL is the
// canonical "nats://host:port" URL.
func Connect(ctx context.Context, natsURL, site string) (Publisher, error) {
	opts := []nats.Option{
		nats.Name("docker-host-broker"),
		nats.MaxReconnects(-1),                  // reconnect forever
		nats.ReconnectWait(2 * time.Second),
		nats.ReconnectBufSize(8 * 1024 * 1024),  // 8 MB buffer during disconnect
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("natspub: disconnected: %v", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("natspub: reconnected to %s", c.ConnectedUrl())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Printf("natspub: async error: %v", err)
		}),
	}
	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("natspub: Connect(%s): %w", natsURL, err)
	}
	return &realPublisher{nc: nc, subjects: subjectsFor(site)}, nil
}

func (p *realPublisher) Close() error {
	// Drain blocks until pending messages are delivered, with a
	// reasonable upper bound. nats.go default DrainTimeout is 30s.
	return p.nc.Drain()
}

// publishJSON marshals payload and publishes to subject. Errors are
// logged but not propagated upward — there's no useful action a
// publisher loop can take besides "try again next tick."
func (p *realPublisher) publishJSON(subject string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("natspub: marshal %s: %w", subject, err)
	}
	if err := p.nc.Publish(subject, b); err != nil {
		return fmt.Errorf("natspub: publish %s: %w", subject, err)
	}
	return nil
}

// --- per-subject envelopes ----------------------------------------------------

type heartbeatEnvelope struct {
	TS              float64 `json:"ts"`
	Seq             uint64  `json:"seq"`
	BrokerVersion   string  `json:"broker_version"`
	UptimeS         float64 `json:"uptime_s"`
	DockerSocketOK  bool    `json:"docker_socket_ok"`
}

type snapshotEnvelope struct {
	TS         float64                    `json:"ts"`
	Seq        uint64                     `json:"seq"`
	Containers []dockercli.ContainerInfo  `json:"containers"`
}

type deltaEnvelope struct {
	TS     float64            `json:"ts"`
	Seq    uint64             `json:"seq"`
	Events []state.DeltaEvent `json:"events"`
}

type statsEnvelope struct {
	TS    float64                  `json:"ts"`
	Seq   uint64                   `json:"seq"`
	Stats map[string]state.Stats   `json:"stats"`
}

type hostMetricsEnvelope struct {
	TS   float64           `json:"ts"`
	Seq  uint64            `json:"seq"`
	Host state.HostMetrics `json:"host"`
}

func tsFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// --- public publish methods ---------------------------------------------------

func (p *realPublisher) PublishHeartbeat(
	ts time.Time, seq uint64, dockerOK bool, uptime time.Duration, version string,
) error {
	return p.publishJSON(p.subjects.heartbeat, heartbeatEnvelope{
		TS:             tsFloat(ts),
		Seq:            seq,
		BrokerVersion:  version,
		UptimeS:        uptime.Seconds(),
		DockerSocketOK: dockerOK,
	})
}

func (p *realPublisher) PublishContainersSnapshot(
	ts time.Time, seq uint64, containers []dockercli.ContainerInfo,
) error {
	return p.publishJSON(p.subjects.snapshot, snapshotEnvelope{
		TS:         tsFloat(ts),
		Seq:        seq,
		Containers: containers,
	})
}

func (p *realPublisher) PublishContainersDelta(
	ts time.Time, seq uint64, events []state.DeltaEvent,
) error {
	if len(events) == 0 {
		return nil // no-op when there's nothing to report
	}
	return p.publishJSON(p.subjects.delta, deltaEnvelope{
		TS:     tsFloat(ts),
		Seq:    seq,
		Events: events,
	})
}

func (p *realPublisher) PublishContainersStats(
	ts time.Time, seq uint64, stats map[string]state.Stats,
) error {
	return p.publishJSON(p.subjects.stats, statsEnvelope{
		TS:    tsFloat(ts),
		Seq:   seq,
		Stats: stats,
	})
}

func (p *realPublisher) PublishHostMetrics(
	ts time.Time, seq uint64, host state.HostMetrics,
) error {
	return p.publishJSON(p.subjects.hostMetrics, hostMetricsEnvelope{
		TS:   tsFloat(ts),
		Seq:  seq,
		Host: host,
	})
}
