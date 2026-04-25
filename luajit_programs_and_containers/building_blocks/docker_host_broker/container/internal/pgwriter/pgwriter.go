// Package pgwriter mirrors the broker's published state into the
// knowledge_base_status table so consumers that already read pg via
// kb_status (notably bare-LuaJIT dcs.lua) get broker state without
// needing a NATS client.
//
// Wire-protocol parity: payloads written here match the NATS envelopes
// byte-for-byte (same json.Marshal calls). Consumers that read from
// either source see the same shape.
//
// Path scheme — every snapshot under:
//
//   system.site.<site>.docker_broker.containers.KB_STATUS_FIELD.snapshot
//   system.site.<site>.docker_broker.containers.KB_STATUS_FIELD.stats
//   system.site.<site>.docker_broker.heartbeat.KB_STATUS_FIELD.last
//   system.site.<site>.docker_broker.host_metrics.KB_STATUS_FIELD.snapshot
//
// All writes are INSERT ... ON CONFLICT (path) DO UPDATE — self-healing,
// no construction-time row pre-allocation required.
package pgwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
	"github.com/nanodatacenter/docker_host_broker/internal/state"
)

// Writer wraps a pgx connection pool and emits broker state into
// knowledge_base_status.
type Writer struct {
	pool  *pgxpool.Pool
	paths paths
}

type paths struct {
	containers  string
	stats       string
	heartbeat   string
	hostMetrics string
}

func pathsFor(site string) paths {
	root := "system.site." + site + ".docker_broker."
	return paths{
		containers:  root + "containers.KB_STATUS_FIELD.snapshot",
		stats:       root + "containers.KB_STATUS_FIELD.stats",
		heartbeat:   root + "heartbeat.KB_STATUS_FIELD.last",
		hostMetrics: root + "host_metrics.KB_STATUS_FIELD.snapshot",
	}
}

// Connect dials pg using the given DSN
// (e.g., "host=pg-vector port=5432 user=gedgar dbname=knowledge_base password=...").
// Returns a Writer ready for use.
func Connect(ctx context.Context, dsn, site string) (*Writer, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgwriter: parse DSN: %w", err)
	}
	// Small pool — broker writes serially per subject.
	cfg.MaxConns = 4
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgwriter: pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgwriter: ping: %w", err)
	}
	return &Writer{pool: pool, paths: pathsFor(site)}, nil
}

func (w *Writer) Close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

const upsertSQL = `
INSERT INTO knowledge_base_status (path, data)
VALUES ($1::ltree, $2::json)
ON CONFLICT (path) DO UPDATE
SET data = EXCLUDED.data
`

func (w *Writer) upsert(ctx context.Context, path string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pgwriter: marshal %s: %w", path, err)
	}
	if _, err := w.pool.Exec(ctx, upsertSQL, path, string(b)); err != nil {
		return fmt.Errorf("pgwriter: upsert %s: %w", path, err)
	}
	return nil
}

// --- envelopes (parallel to natspub envelopes — same JSON shape) ---

type heartbeatEnvelope struct {
	TS             float64 `json:"ts"`
	Seq            uint64  `json:"seq"`
	BrokerVersion  string  `json:"broker_version"`
	UptimeS        float64 `json:"uptime_s"`
	DockerSocketOK bool    `json:"docker_socket_ok"`
}

type snapshotEnvelope struct {
	TS         float64                   `json:"ts"`
	Seq        uint64                    `json:"seq"`
	Containers []dockercli.ContainerInfo `json:"containers"`
}

type statsEnvelope struct {
	TS    float64                `json:"ts"`
	Seq   uint64                 `json:"seq"`
	Stats map[string]state.Stats `json:"stats"`
}

type hostMetricsEnvelope struct {
	TS   float64           `json:"ts"`
	Seq  uint64            `json:"seq"`
	Host state.HostMetrics `json:"host"`
}

func tsFloat(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// --- public write methods ---

func (w *Writer) WriteHeartbeat(
	ctx context.Context,
	ts time.Time, seq uint64, dockerOK bool, uptime time.Duration, version string,
) error {
	return w.upsert(ctx, w.paths.heartbeat, heartbeatEnvelope{
		TS:             tsFloat(ts),
		Seq:            seq,
		BrokerVersion:  version,
		UptimeS:        uptime.Seconds(),
		DockerSocketOK: dockerOK,
	})
}

func (w *Writer) WriteContainersSnapshot(
	ctx context.Context,
	ts time.Time, seq uint64, containers []dockercli.ContainerInfo,
) error {
	return w.upsert(ctx, w.paths.containers, snapshotEnvelope{
		TS: tsFloat(ts), Seq: seq, Containers: containers,
	})
}

func (w *Writer) WriteContainersStats(
	ctx context.Context,
	ts time.Time, seq uint64, stats map[string]state.Stats,
) error {
	return w.upsert(ctx, w.paths.stats, statsEnvelope{
		TS: tsFloat(ts), Seq: seq, Stats: stats,
	})
}

func (w *Writer) WriteHostMetrics(
	ctx context.Context,
	ts time.Time, seq uint64, host state.HostMetrics,
) error {
	return w.upsert(ctx, w.paths.hostMetrics, hostMetricsEnvelope{
		TS: tsFloat(ts), Seq: seq, Host: host,
	})
}
