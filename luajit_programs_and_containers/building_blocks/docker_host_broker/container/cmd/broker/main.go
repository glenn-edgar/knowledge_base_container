// docker-host-broker entry point.
//
// Wires the four poll loops (containers, stats, heartbeat, http) and
// performs graceful shutdown on SIGINT/SIGTERM.
//
// See WIRE_PROTOCOL.md for the contract this broker implements.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
	"github.com/nanodatacenter/docker_host_broker/internal/httpapi"
	"github.com/nanodatacenter/docker_host_broker/internal/natspub"
	"github.com/nanodatacenter/docker_host_broker/internal/pgwriter"
	"github.com/nanodatacenter/docker_host_broker/internal/state"
)

const brokerVersion = "0.2.0-phase2"

type config struct {
	site             string
	natsURL          string
	pgDSN            string
	httpAddr         string
	dockerHost       string
	pollContainersS  int
	pollStatsS       int
	heartbeatS       int
}

func loadConfig() config {
	getenvOrDefault := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	getenvIntOrDefault := func(key string, def int) int {
		if v := os.Getenv(key); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
		return def
	}
	return config{
		site:            getenvOrDefault("SITE", "moonbase.alpha"),
		natsURL:         getenvOrDefault("NATS_URL", "nats://nats-js-ram:4222"),
		pgDSN:           os.Getenv("PG_DSN"), // empty disables pg mirror
		httpAddr:        getenvOrDefault("HTTP_ADDR", "127.0.0.1:9100"),
		dockerHost:      getenvOrDefault("DOCKER_HOST", "unix:///var/run/docker.sock"),
		pollContainersS: getenvIntOrDefault("POLL_CONTAINERS_S", 5),
		pollStatsS:      getenvIntOrDefault("POLL_STATS_S", 5),
		heartbeatS:      getenvIntOrDefault("HEARTBEAT_S", 1),
	}
}

func main() {
	flag.Parse()
	cfg := loadConfig()

	log.Printf("docker-host-broker v%s starting", brokerVersion)
	log.Printf("  site=%s nats=%s http=%s docker=%s",
		cfg.site, cfg.natsURL, cfg.httpAddr, cfg.dockerHost)
	log.Printf("  cadences (s): containers=%d stats=%d heartbeat=%d",
		cfg.pollContainersS, cfg.pollStatsS, cfg.heartbeatS)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- subsystems ---
	dock, err := dockercli.New(ctx, cfg.dockerHost)
	if err != nil {
		log.Fatalf("dockercli connect: %v", err)
	}
	defer dock.Close()

	cache := state.NewCache()
	status := state.NewStatus(brokerVersion)
	status.SetDockerSocketOK(true) // Ping succeeded inside dockercli.New

	pub, err := natspub.Connect(ctx, cfg.natsURL, cfg.site)
	if err != nil {
		log.Fatalf("natspub connect: %v", err)
	}
	defer pub.Close()

	// pgwriter is optional — broker runs in NATS-only mode if PG_DSN is empty.
	// When configured, broker mirrors every published payload into
	// knowledge_base_status so dcs.lua (bare LuaJIT, no NATS client) can
	// read broker state via its existing kb_status helpers.
	var pgw *pgwriter.Writer
	if cfg.pgDSN != "" {
		pgw, err = pgwriter.Connect(ctx, cfg.pgDSN, cfg.site)
		if err != nil {
			log.Printf("pgwriter connect failed (continuing in NATS-only mode): %v", err)
			pgw = nil
		} else {
			log.Printf("pgwriter connected; mirroring state into knowledge_base_status")
			defer pgw.Close()
		}
	} else {
		log.Printf("pgwriter disabled (PG_DSN unset); NATS-only mode")
	}

	httpSrv := httpapi.New(cfg.httpAddr, cache, status, dock)

	// --- goroutine loops ---
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		runContainerPoll(ctx, dock, cache, pub, pgw, status, time.Duration(cfg.pollContainersS)*time.Second)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runStatsPoll(ctx, dock, cache, pub, pgw, time.Duration(cfg.pollStatsS)*time.Second)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runHeartbeat(ctx, pub, pgw, status, time.Duration(cfg.heartbeatS)*time.Second)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := httpSrv.Run(ctx); err != nil {
			log.Printf("http: %v", err)
		}
	}()

	// --- signal handling ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received signal=%s; shutting down", sig)
	cancel()

	// Bound shutdown so we can't hang forever.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Printf("clean shutdown")
	case <-time.After(10 * time.Second):
		log.Printf("shutdown timed out after 10s; exiting anyway")
	}
}

// runContainerPoll fires immediately, then on each tick: list + inspect
// every container in parallel, push into cache, publish snapshot and
// any deltas. pgw may be nil — in that case the pg mirror is skipped.
func runContainerPoll(
	ctx context.Context,
	dock dockercli.Client,
	cache *state.Cache,
	pub natspub.Publisher,
	pgw *pgwriter.Writer,
	status *state.Status,
	period time.Duration,
) {
	tick := func() {
		now := time.Now()
		list, err := dock.ListContainers(ctx)
		if err != nil {
			log.Printf("container poll: ListContainers: %v", err)
			status.SetDockerSocketOK(false)
			return
		}
		// Fan out InspectContainer for full per-container detail.
		var mu sync.Mutex
		full := make([]dockercli.ContainerInfo, 0, len(list))
		var iwg sync.WaitGroup
		for _, summary := range list {
			if summary.Name == "" {
				continue
			}
			iwg.Add(1)
			go func(name string, fallback dockercli.ContainerInfo) {
				defer iwg.Done()
				inspectCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				ci, err := dock.InspectContainer(inspectCtx, name)
				if err != nil {
					log.Printf("container poll: Inspect(%s): %v", name, err)
					mu.Lock()
					full = append(full, fallback)
					mu.Unlock()
					return
				}
				mu.Lock()
				full = append(full, ci)
				mu.Unlock()
			}(summary.Name, summary)
		}
		iwg.Wait()
		status.SetDockerSocketOK(true)

		deltas := cache.UpdateContainers(now, full)
		ts, seq, snap := cache.SnapshotContainers()
		if err := pub.PublishContainersSnapshot(ts, seq, snap); err != nil {
			log.Printf("container poll: publish snapshot: %v", err)
		}
		if err := pub.PublishContainersDelta(ts, seq, deltas); err != nil {
			log.Printf("container poll: publish delta: %v", err)
		}
		if pgw != nil {
			pgCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := pgw.WriteContainersSnapshot(pgCtx, ts, seq, snap); err != nil {
				log.Printf("container poll: pg upsert snapshot: %v", err)
			}
			cancel()
		}
	}

	tick()
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}

// runStatsPoll iterates the currently-running containers (from cache)
// and samples each in parallel. pgw may be nil to skip pg mirroring.
func runStatsPoll(
	ctx context.Context,
	dock dockercli.Client,
	cache *state.Cache,
	pub natspub.Publisher,
	pgw *pgwriter.Writer,
	period time.Duration,
) {
	tick := func() {
		_, _, snap := cache.SnapshotContainers()
		if len(snap) == 0 {
			return
		}
		var mu sync.Mutex
		raw := map[string]dockercli.RawStats{}
		var swg sync.WaitGroup
		for _, ci := range snap {
			if ci.State != "running" {
				continue
			}
			swg.Add(1)
			go func(name string) {
				defer swg.Done()
				statsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				rs, err := dock.Stats(statsCtx, name)
				if err != nil {
					log.Printf("stats poll: Stats(%s): %v", name, err)
					return
				}
				mu.Lock()
				raw[name] = rs
				mu.Unlock()
			}(ci.Name)
		}
		swg.Wait()
		now := time.Now()
		cache.UpdateStats(now, raw)
		ts, seq, stats := cache.SnapshotStats()
		if err := pub.PublishContainersStats(ts, seq, stats); err != nil {
			log.Printf("stats poll: publish: %v", err)
		}
		if pgw != nil {
			pgCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := pgw.WriteContainersStats(pgCtx, ts, seq, stats); err != nil {
				log.Printf("stats poll: pg upsert: %v", err)
			}
			cancel()
		}
	}

	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}

// runHeartbeat publishes the broker liveness signal at heartbeatS cadence.
// pgw may be nil to skip pg mirroring.
func runHeartbeat(
	ctx context.Context,
	pub natspub.Publisher,
	pgw *pgwriter.Writer,
	status *state.Status,
	period time.Duration,
) {
	tick := func() {
		seq := status.IncHeartbeatSeq()
		now := time.Now()
		dockerOK := status.DockerSocketOK()
		uptime := status.Uptime()
		version := status.BrokerVersion()
		if err := pub.PublishHeartbeat(now, seq, dockerOK, uptime, version); err != nil {
			log.Printf("heartbeat: %v", err)
		}
		if pgw != nil {
			pgCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := pgw.WriteHeartbeat(pgCtx, now, seq, dockerOK, uptime, version); err != nil {
				log.Printf("heartbeat: pg upsert: %v", err)
			}
			cancel()
		}
	}

	tick()
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}
