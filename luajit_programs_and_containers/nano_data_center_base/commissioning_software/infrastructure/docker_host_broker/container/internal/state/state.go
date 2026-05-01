// Package state holds the in-memory cache of container + host metric
// snapshots and computes deltas between consecutive container polls.
//
// Pure Go: no Docker SDK, no NATS, no HTTP. Testable in isolation.
package state

import (
	"sync"
	"time"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
)

// Cache is the broker's authoritative in-memory store. Every poll
// loop writes to it; every NATS publish + HTTP read serves from it.
type Cache struct {
	mu sync.RWMutex

	containersTS  time.Time
	containersSeq uint64
	containers    map[string]dockercli.ContainerInfo // keyed by name

	hostMetricsTS  time.Time
	hostMetricsSeq uint64
	hostMetrics    HostMetrics

	statsTS  time.Time
	statsSeq uint64
	stats    map[string]Stats

	prevRawStats map[string]dockercli.RawStats
}

// HostMetrics mirrors the wire-protocol host_metrics shape.
type HostMetrics struct {
	CPUPct      float64            `json:"cpu_pct"`
	Load1m      float64            `json:"load_1m"`
	Load5m      float64            `json:"load_5m"`
	Load15m     float64            `json:"load_15m"`
	MemTotalMB  uint64             `json:"mem_total_mb"`
	MemUsedMB   uint64             `json:"mem_used_mb"`
	MemFreeMB   uint64             `json:"mem_free_mb"`
	MemBuffMB   uint64             `json:"mem_buffers_mb"`
	MemCacheMB  uint64             `json:"mem_cached_mb"`
	SwapUsedMB  uint64             `json:"swap_used_mb"`
	SwapTotalMB uint64             `json:"swap_total_mb"`
	NetRxKbps   float64            `json:"net_rx_kbps"`
	NetTxKbps   float64            `json:"net_tx_kbps"`
	DiskUsedPct map[string]float64 `json:"disk_used_pct"`
}

// Stats mirrors the wire-protocol per-container stats shape.
type Stats struct {
	CPUPct        float64 `json:"cpu_pct"`
	MemRSSMB      float64 `json:"mem_rss_mb"`
	MemLimitMB    float64 `json:"mem_limit_mb"`
	DiskReadKbps  float64 `json:"disk_read_kbps"`
	DiskWriteKbps float64 `json:"disk_write_kbps"`
	NetRxKbps     float64 `json:"net_rx_kbps"`
	NetTxKbps     float64 `json:"net_tx_kbps"`
}

// DeltaEvent represents one change between two consecutive container
// snapshots.
type DeltaEvent struct {
	Kind   string `json:"kind"` // "state_change" | "appeared" | "disappeared" | "health_change" | "exit_code_change"
	Name   string `json:"name"`
	ID     string `json:"id"`
	Before string `json:"before"`
	After  string `json:"after"`
}

// NewCache returns an empty cache.
func NewCache() *Cache {
	return &Cache{
		containers:   map[string]dockercli.ContainerInfo{},
		stats:        map[string]Stats{},
		prevRawStats: map[string]dockercli.RawStats{},
		hostMetrics:  HostMetrics{DiskUsedPct: map[string]float64{}},
	}
}

// UpdateContainers replaces the container snapshot and returns deltas
// vs. the previous snapshot. The returned slice is safe for the caller
// to retain (a fresh slice on each call).
func (c *Cache) UpdateContainers(ts time.Time, list []dockercli.ContainerInfo) []DeltaEvent {
	next := make(map[string]dockercli.ContainerInfo, len(list))
	for _, ci := range list {
		if ci.Name == "" {
			continue
		}
		next[ci.Name] = ci
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	deltas := diffContainers(c.containers, next)
	c.containers = next
	c.containersTS = ts
	c.containersSeq++
	return deltas
}

// diffContainers detects events between previous and next.
func diffContainers(prev, next map[string]dockercli.ContainerInfo) []DeltaEvent {
	out := []DeltaEvent{}

	for name, n := range next {
		p, existed := prev[name]
		if !existed {
			out = append(out, DeltaEvent{Kind: "appeared", Name: name, ID: n.ID})
			continue
		}
		if p.State != n.State {
			out = append(out, DeltaEvent{
				Kind: "state_change", Name: name, ID: n.ID,
				Before: p.State, After: n.State,
			})
		}
		if p.Health != n.Health {
			out = append(out, DeltaEvent{
				Kind: "health_change", Name: name, ID: n.ID,
				Before: p.Health, After: n.Health,
			})
		}
		if exitCodeChanged(p.ExitCode, n.ExitCode) {
			out = append(out, DeltaEvent{
				Kind: "exit_code_change", Name: name, ID: n.ID,
				Before: exitCodeStr(p.ExitCode),
				After:  exitCodeStr(n.ExitCode),
			})
		}
	}

	for name, p := range prev {
		if _, ok := next[name]; !ok {
			out = append(out, DeltaEvent{Kind: "disappeared", Name: name, ID: p.ID})
		}
	}
	return out
}

func exitCodeChanged(a, b *int) bool {
	if a == nil && b == nil {
		return false
	}
	if (a == nil) != (b == nil) {
		return true
	}
	return *a != *b
}

func exitCodeStr(p *int) string {
	if p == nil {
		return ""
	}
	return itoa(*p)
}

// itoa returns base-10 string for a possibly-negative int. Avoids
// strconv import for nano-package hygiene.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// UpdateHostMetrics replaces the host metrics snapshot.
func (c *Cache) UpdateHostMetrics(ts time.Time, m HostMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hostMetrics = m
	c.hostMetricsTS = ts
	c.hostMetricsSeq++
}

// UpdateStats accepts per-container raw cumulative counters and
// computes rates against the previous sample.
func (c *Cache) UpdateStats(ts time.Time, raw map[string]dockercli.RawStats) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make(map[string]Stats, len(raw))
	for name, cur := range raw {
		prev, hasPrev := c.prevRawStats[name]
		if !hasPrev {
			// First sample for this container: counters known but rates are
			// not yet meaningful. Emit zeros for rates and a real mem reading.
			out[name] = Stats{
				CPUPct:     0.0,
				MemRSSMB:   bytesToMB(cur.MemUsage),
				MemLimitMB: bytesToMB(cur.MemLimit),
			}
			c.prevRawStats[name] = cur
			continue
		}
		out[name] = computeStats(prev, cur)
		c.prevRawStats[name] = cur
	}

	// Drop prevRawStats entries for containers that no longer report.
	for name := range c.prevRawStats {
		if _, ok := raw[name]; !ok {
			delete(c.prevRawStats, name)
		}
	}

	c.stats = out
	c.statsTS = ts
	c.statsSeq++
}

// computeStats derives wire-protocol Stats from two consecutive raw samples.
//
// CPU% normalization (per feedback_docker_stats_cpu_semantics.md):
// 100% = 1 core. So a container fully using 1 of 8 cores reports 100%,
// not 12.5%. The classic docker-stats formula multiplies by NumCPUs to
// reach this convention.
func computeStats(prev, cur dockercli.RawStats) Stats {
	dt := cur.SampleAt.Sub(prev.SampleAt).Seconds()
	if dt <= 0 {
		dt = 1.0
	}

	cpuPct := 0.0
	cpuDelta := float64(cur.CPUUsage) - float64(prev.CPUUsage)
	sysDelta := float64(cur.SystemUsage) - float64(prev.SystemUsage)
	numCPUs := cur.NumCPUs
	if numCPUs == 0 {
		numCPUs = 1
	}
	if cpuDelta > 0 && sysDelta > 0 {
		cpuPct = (cpuDelta / sysDelta) * float64(numCPUs) * 100.0
	}

	dr := rateKBps(prev.BlkRead, cur.BlkRead, dt)
	dw := rateKBps(prev.BlkWrite, cur.BlkWrite, dt)
	nr := rateKBps(prev.NetRx, cur.NetRx, dt)
	nt := rateKBps(prev.NetTx, cur.NetTx, dt)

	return Stats{
		CPUPct:        cpuPct,
		MemRSSMB:      bytesToMB(cur.MemUsage),
		MemLimitMB:    bytesToMB(cur.MemLimit),
		DiskReadKbps:  dr,
		DiskWriteKbps: dw,
		NetRxKbps:     nr,
		NetTxKbps:     nt,
	}
}

// rateKBps computes a positive rate in KB/sec between two cumulative
// counters. Counter resets (cur < prev, e.g., after container restart)
// produce 0, not negative numbers.
func rateKBps(prev, cur uint64, dt float64) float64 {
	if cur < prev || dt <= 0 {
		return 0
	}
	return float64(cur-prev) / dt / 1024.0
}

func bytesToMB(b uint64) float64 {
	return float64(b) / (1024.0 * 1024.0)
}

// SnapshotContainers returns a deep copy of the current containers
// for emitting via NATS or HTTP.
func (c *Cache) SnapshotContainers() (ts time.Time, seq uint64, out []dockercli.ContainerInfo) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out = make([]dockercli.ContainerInfo, 0, len(c.containers))
	for _, ci := range c.containers {
		out = append(out, copyContainerInfo(ci))
	}
	return c.containersTS, c.containersSeq, out
}

// SnapshotContainer returns a single container by name. ok is false
// if not found.
func (c *Cache) SnapshotContainer(name string) (ts time.Time, seq uint64, ci dockercli.ContainerInfo, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	got, found := c.containers[name]
	if !found {
		return c.containersTS, c.containersSeq, dockercli.ContainerInfo{}, false
	}
	return c.containersTS, c.containersSeq, copyContainerInfo(got), true
}

// SnapshotHostMetrics returns the current host metrics snapshot.
func (c *Cache) SnapshotHostMetrics() (ts time.Time, seq uint64, out HostMetrics) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hm := c.hostMetrics
	hm.DiskUsedPct = make(map[string]float64, len(c.hostMetrics.DiskUsedPct))
	for k, v := range c.hostMetrics.DiskUsedPct {
		hm.DiskUsedPct[k] = v
	}
	return c.hostMetricsTS, c.hostMetricsSeq, hm
}

// SnapshotStats returns a deep copy of the current per-container stats.
func (c *Cache) SnapshotStats() (ts time.Time, seq uint64, out map[string]Stats) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out = make(map[string]Stats, len(c.stats))
	for k, v := range c.stats {
		out[k] = v
	}
	return c.statsTS, c.statsSeq, out
}

// copyContainerInfo deep-copies maps + slices so callers can mutate
// safely without racing the cache's internal state.
func copyContainerInfo(ci dockercli.ContainerInfo) dockercli.ContainerInfo {
	cp := ci
	cp.Labels = make(map[string]string, len(ci.Labels))
	for k, v := range ci.Labels {
		cp.Labels[k] = v
	}
	cp.IPAddresses = make(map[string]string, len(ci.IPAddresses))
	for k, v := range ci.IPAddresses {
		cp.IPAddresses[k] = v
	}
	cp.Ports = make([]dockercli.PortBinding, len(ci.Ports))
	copy(cp.Ports, ci.Ports)
	if ci.ExitCode != nil {
		ec := *ci.ExitCode
		cp.ExitCode = &ec
	}
	return cp
}
