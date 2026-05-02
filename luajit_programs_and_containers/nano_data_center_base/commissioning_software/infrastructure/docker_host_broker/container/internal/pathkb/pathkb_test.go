package pathkb

import (
	"strings"
	"testing"
)

// TestMain configures the package once before all tests in the table run.
// SystemName() is process-wide state; tests share it.
func TestMain(m *testing.M) {
	if err := Configure("moon_base"); err != nil {
		panic(err)
	}
	m.Run()
}

func TestSiteRoot(t *testing.T) {
	got := SiteRoot("moon_base_alpha")
	want := "system.moon_base.site.moon_base_alpha"
	if got != want {
		t.Fatalf("SiteRoot: got %q want %q", got, want)
	}
}

func TestSitePathSuffix(t *testing.T) {
	got := SitePath("moon_base_alpha", "KB_STATUS_FIELD.system_ready")
	want := "system.moon_base.site.moon_base_alpha.KB_STATUS_FIELD.system_ready"
	if got != want {
		t.Fatalf("SitePath: got %q want %q", got, want)
	}
}

func TestSitePathEmpty(t *testing.T) {
	got := SitePath("moon_base_alpha", "")
	want := "system.moon_base.site.moon_base_alpha"
	if got != want {
		t.Fatalf("SitePath empty suffix: got %q want %q", got, want)
	}
}

func TestBrokerRoot(t *testing.T) {
	got := BrokerRoot("moon_base_alpha")
	want := "system.moon_base.site.moon_base_alpha.docker_broker."
	if got != want {
		t.Fatalf("BrokerRoot: got %q want %q", got, want)
	}
}

// Match the existing pgwriter/natspub call shape: BrokerRoot ends in '.'
// so callers can directly concatenate leaf keys.
func TestBrokerRootConcatenation(t *testing.T) {
	root := BrokerRoot("moon_base_alpha")
	cases := map[string]string{
		root + "containers.KB_STATUS_FIELD.snapshot":   "system.moon_base.site.moon_base_alpha.docker_broker.containers.KB_STATUS_FIELD.snapshot",
		root + "containers.KB_STATUS_FIELD.stats":      "system.moon_base.site.moon_base_alpha.docker_broker.containers.KB_STATUS_FIELD.stats",
		root + "heartbeat.KB_STATUS_FIELD.last":        "system.moon_base.site.moon_base_alpha.docker_broker.heartbeat.KB_STATUS_FIELD.last",
		root + "host_metrics.KB_STATUS_FIELD.snapshot": "system.moon_base.site.moon_base_alpha.docker_broker.host_metrics.KB_STATUS_FIELD.snapshot",
		root + "heartbeat":                             "system.moon_base.site.moon_base_alpha.docker_broker.heartbeat",
		root + "containers.snapshot":                   "system.moon_base.site.moon_base_alpha.docker_broker.containers.snapshot",
		root + "containers.delta":                      "system.moon_base.site.moon_base_alpha.docker_broker.containers.delta",
		root + "containers.stats":                      "system.moon_base.site.moon_base_alpha.docker_broker.containers.stats",
		root + "host_metrics.snapshot":                 "system.moon_base.site.moon_base_alpha.docker_broker.host_metrics.snapshot",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	}
}

// Sanity: every produced path starts with "system.<sys>.site." and never
// reverts to the legacy "system.site." shape.
func TestNoLegacyShape(t *testing.T) {
	checks := []string{
		SiteRoot("any_site"),
		SitePath("any_site", "x.y"),
		BrokerRoot("any_site"),
	}
	for _, p := range checks {
		if strings.HasPrefix(p, "system.site.") {
			t.Errorf("legacy shape leaked: %q starts with system.site.", p)
		}
		if !strings.HasPrefix(p, "system.moon_base.site.") {
			t.Errorf("expected system.moon_base.site. prefix, got %q", p)
		}
	}
}
