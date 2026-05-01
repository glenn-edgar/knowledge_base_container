package pathkb

import "testing"

func TestSiteRoot(t *testing.T) {
	got := SiteRoot("moonbase.alpha.dcs")
	want := "system.site.moonbase.alpha.dcs"
	if got != want {
		t.Fatalf("SiteRoot: got %q want %q", got, want)
	}
}

func TestSitePathSuffix(t *testing.T) {
	got := SitePath("moonbase.alpha.dcs", "KB_STATUS_FIELD.system_ready")
	want := "system.site.moonbase.alpha.dcs.KB_STATUS_FIELD.system_ready"
	if got != want {
		t.Fatalf("SitePath: got %q want %q", got, want)
	}
}

func TestSitePathEmpty(t *testing.T) {
	got := SitePath("moonbase.alpha.dcs", "")
	want := "system.site.moonbase.alpha.dcs"
	if got != want {
		t.Fatalf("SitePath empty suffix: got %q want %q", got, want)
	}
}

func TestBrokerRoot(t *testing.T) {
	got := BrokerRoot("moonbase.alpha.dcs")
	want := "system.site.moonbase.alpha.dcs.docker_broker."
	if got != want {
		t.Fatalf("BrokerRoot: got %q want %q", got, want)
	}
}

// Match the existing pgwriter/natspub call shape: BrokerRoot ends in '.'
// so callers can directly concatenate leaf keys.
func TestBrokerRootConcatenation(t *testing.T) {
	root := BrokerRoot("moonbase.alpha.dcs")
	cases := map[string]string{
		root + "containers.KB_STATUS_FIELD.snapshot":   "system.site.moonbase.alpha.dcs.docker_broker.containers.KB_STATUS_FIELD.snapshot",
		root + "containers.KB_STATUS_FIELD.stats":      "system.site.moonbase.alpha.dcs.docker_broker.containers.KB_STATUS_FIELD.stats",
		root + "heartbeat.KB_STATUS_FIELD.last":        "system.site.moonbase.alpha.dcs.docker_broker.heartbeat.KB_STATUS_FIELD.last",
		root + "host_metrics.KB_STATUS_FIELD.snapshot": "system.site.moonbase.alpha.dcs.docker_broker.host_metrics.KB_STATUS_FIELD.snapshot",
		root + "heartbeat":                             "system.site.moonbase.alpha.dcs.docker_broker.heartbeat",
		root + "containers.snapshot":                   "system.site.moonbase.alpha.dcs.docker_broker.containers.snapshot",
		root + "containers.delta":                      "system.site.moonbase.alpha.dcs.docker_broker.containers.delta",
		root + "containers.stats":                      "system.site.moonbase.alpha.dcs.docker_broker.containers.stats",
		root + "host_metrics.snapshot":                 "system.site.moonbase.alpha.dcs.docker_broker.host_metrics.snapshot",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	}
}
