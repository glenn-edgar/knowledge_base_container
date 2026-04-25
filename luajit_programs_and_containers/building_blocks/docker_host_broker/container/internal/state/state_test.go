package state

import (
	"sort"
	"testing"
	"time"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
)

func ci(name, state, health string, exitCode *int) dockercli.ContainerInfo {
	return dockercli.ContainerInfo{
		ID:     name + "-id",
		Name:   name,
		State:  state,
		Health: health,
		ExitCode: exitCode,
	}
}

func intp(n int) *int { return &n }

// sortDeltas makes test assertions order-insensitive.
func sortDeltas(d []DeltaEvent) {
	sort.Slice(d, func(i, j int) bool {
		if d[i].Kind != d[j].Kind {
			return d[i].Kind < d[j].Kind
		}
		return d[i].Name < d[j].Name
	})
}

func TestUpdateContainers_Appearance(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	deltas := c.UpdateContainers(t0, []dockercli.ContainerInfo{
		ci("pg-vector", "running", "healthy", nil),
	})
	if len(deltas) != 1 {
		t.Fatalf("want 1 delta, got %d: %#v", len(deltas), deltas)
	}
	if deltas[0].Kind != "appeared" || deltas[0].Name != "pg-vector" {
		t.Errorf("wrong delta: %#v", deltas[0])
	}
}

func TestUpdateContainers_Disappearance(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	c.UpdateContainers(t0, []dockercli.ContainerInfo{
		ci("pg-vector", "running", "healthy", nil),
	})
	deltas := c.UpdateContainers(t0.Add(time.Second), []dockercli.ContainerInfo{})
	if len(deltas) != 1 || deltas[0].Kind != "disappeared" {
		t.Fatalf("want 1 disappeared delta, got %#v", deltas)
	}
}

func TestUpdateContainers_StateChange(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	c.UpdateContainers(t0, []dockercli.ContainerInfo{
		ci("nats-js-ram", "running", "healthy", nil),
	})
	deltas := c.UpdateContainers(t0.Add(time.Second), []dockercli.ContainerInfo{
		ci("nats-js-ram", "exited", "none", intp(0)),
	})
	sortDeltas(deltas)
	if len(deltas) != 3 {
		t.Fatalf("want 3 deltas (state+health+exit), got %d: %#v", len(deltas), deltas)
	}
	want := map[string]string{
		"exit_code_change": "0", // After carries the new exit code as string
		"health_change":    "none",
		"state_change":     "exited",
	}
	for _, d := range deltas {
		if w, ok := want[d.Kind]; !ok || d.After != w {
			t.Errorf("unexpected delta: %#v", d)
		}
	}
}

func TestUpdateContainers_NoChange(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	infos := []dockercli.ContainerInfo{
		ci("pg-vector", "running", "healthy", nil),
		ci("nats-js-ram", "running", "healthy", nil),
	}
	c.UpdateContainers(t0, infos)
	deltas := c.UpdateContainers(t0.Add(time.Second), infos)
	if len(deltas) != 0 {
		t.Errorf("want 0 deltas on identical snapshot, got %d: %#v", len(deltas), deltas)
	}
}

func TestSnapshotContainers_DeepCopy(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	c.UpdateContainers(t0, []dockercli.ContainerInfo{
		{
			Name:   "pg",
			ID:     "abc",
			State:  "running",
			Labels: map[string]string{"role": "infra"},
			Ports: []dockercli.PortBinding{
				{HostPort: 5432, ContainerPort: 5432, Proto: "tcp"},
			},
		},
	})
	_, _, out := c.SnapshotContainers()
	out[0].Labels["role"] = "MUTATED"
	out[0].Ports[0].HostPort = 9999
	_, _, again := c.SnapshotContainers()
	if again[0].Labels["role"] != "infra" {
		t.Errorf("snapshot labels not deep-copied: %v", again[0].Labels)
	}
	if again[0].Ports[0].HostPort != 5432 {
		t.Errorf("snapshot ports not deep-copied: %v", again[0].Ports)
	}
}

func TestUpdateStats_FirstSampleZeroRates(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	raw := map[string]dockercli.RawStats{
		"pg-vector": {
			SampleAt: t0,
			CPUUsage: 1_000_000_000, SystemUsage: 8_000_000_000, NumCPUs: 8,
			MemUsage: 128 * 1024 * 1024, MemLimit: 8 * 1024 * 1024 * 1024,
			BlkRead: 1024, BlkWrite: 2048,
		},
	}
	c.UpdateStats(t0, raw)
	_, _, out := c.SnapshotStats()
	got := out["pg-vector"]
	if got.CPUPct != 0 || got.DiskReadKbps != 0 {
		t.Errorf("first-sample rates should be 0, got %+v", got)
	}
	if got.MemRSSMB < 127 || got.MemRSSMB > 129 {
		t.Errorf("first-sample mem RSS off: %v", got.MemRSSMB)
	}
}

func TestUpdateStats_RatesComputed(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	c.UpdateStats(t0, map[string]dockercli.RawStats{
		"pg-vector": {
			SampleAt: t0,
			CPUUsage: 1_000_000_000, SystemUsage: 8_000_000_000, NumCPUs: 8,
			BlkRead: 0, BlkWrite: 0, NetRx: 0, NetTx: 0,
		},
	})
	// 1s later: container used 1 full core (1B ns), system advanced 8s.
	c.UpdateStats(t0.Add(time.Second), map[string]dockercli.RawStats{
		"pg-vector": {
			SampleAt: t0.Add(time.Second),
			CPUUsage: 2_000_000_000, SystemUsage: 16_000_000_000, NumCPUs: 8,
			BlkRead: 102400, BlkWrite: 204800,
			NetRx: 51200, NetTx: 25600,
		},
	})
	_, _, out := c.SnapshotStats()
	got := out["pg-vector"]
	// (1B / 8B) * 8 cores * 100 = 100% (1 full core)
	if got.CPUPct < 99 || got.CPUPct > 101 {
		t.Errorf("CPU%% should be ~100, got %v", got.CPUPct)
	}
	// 102400 bytes over 1s = 100 KB/s
	if got.DiskReadKbps < 99 || got.DiskReadKbps > 101 {
		t.Errorf("disk_read_kbps should be ~100, got %v", got.DiskReadKbps)
	}
	if got.DiskWriteKbps < 199 || got.DiskWriteKbps > 201 {
		t.Errorf("disk_write_kbps should be ~200, got %v", got.DiskWriteKbps)
	}
	if got.NetRxKbps < 49 || got.NetRxKbps > 51 {
		t.Errorf("net_rx_kbps should be ~50, got %v", got.NetRxKbps)
	}
}

func TestUpdateStats_CounterReset(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	c.UpdateStats(t0, map[string]dockercli.RawStats{
		"pg-vector": {SampleAt: t0, BlkRead: 1_000_000, NumCPUs: 1},
	})
	// counter went DOWN — container restarted; rates must clamp to 0
	c.UpdateStats(t0.Add(time.Second), map[string]dockercli.RawStats{
		"pg-vector": {SampleAt: t0.Add(time.Second), BlkRead: 100, NumCPUs: 1},
	})
	_, _, out := c.SnapshotStats()
	if out["pg-vector"].DiskReadKbps != 0 {
		t.Errorf("counter reset should produce 0 rate, got %v", out["pg-vector"].DiskReadKbps)
	}
}

func TestUpdateStats_DroppedContainerEvictsPrev(t *testing.T) {
	c := NewCache()
	t0 := time.Unix(1714098000, 0)
	c.UpdateStats(t0, map[string]dockercli.RawStats{
		"pg-vector":   {SampleAt: t0, NumCPUs: 1},
		"nats-js-ram": {SampleAt: t0, NumCPUs: 1},
	})
	c.UpdateStats(t0.Add(time.Second), map[string]dockercli.RawStats{
		"pg-vector": {SampleAt: t0.Add(time.Second), NumCPUs: 1},
	})
	if _, ok := c.prevRawStats["nats-js-ram"]; ok {
		t.Errorf("prevRawStats not evicted for missing container")
	}
}
