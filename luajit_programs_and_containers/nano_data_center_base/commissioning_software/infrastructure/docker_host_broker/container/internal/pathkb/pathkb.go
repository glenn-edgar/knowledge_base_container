// Package pathkb composes the DCS knowledge-base ltree path shape used
// by the broker's pg mirror (knowledge_base_status) and NATS subjects.
// Single source of truth for the namespace; mirrors ndc_paths.lua on the
// dcs_host side. Shape: "system.<system_name>.site.<site>.*".
//
// Stateful: Configure() MUST be called once at startup before any path
// emission. system_name is process-wide (a single broker serves a single
// system); site is passed per-call to keep parity with the Lua composer.
package pathkb

import (
	"fmt"
	"sync"
)

var (
	mu         sync.RWMutex
	systemName string
)

// Configure sets the process-wide system_name. Returns an error if called
// twice with a different name (refuse silent reconfiguration); calling
// twice with the same name is a no-op.
func Configure(name string) error {
	if name == "" {
		return fmt.Errorf("pathkb.Configure: system_name required")
	}
	mu.Lock()
	defer mu.Unlock()
	if systemName != "" && systemName != name {
		return fmt.Errorf("pathkb.Configure: already configured with system_name=%q, refusing to reconfigure to %q",
			systemName, name)
	}
	systemName = name
	return nil
}

// SystemName returns the configured system name, or "" if not yet
// configured.
func SystemName() string {
	mu.RLock()
	defer mu.RUnlock()
	return systemName
}

func mustSystemName() string {
	mu.RLock()
	defer mu.RUnlock()
	if systemName == "" {
		panic("pathkb: not configured (call pathkb.Configure(systemName) at startup)")
	}
	return systemName
}

// SiteRoot returns the site-rooted prefix (without trailing dot):
//   "system.<system_name>.site.<site>"
func SiteRoot(site string) string {
	return "system." + mustSystemName() + ".site." + site
}

// SitePath joins a suffix under SiteRoot. Empty suffix returns SiteRoot.
func SitePath(site, suffix string) string {
	if suffix == "" {
		return SiteRoot(site)
	}
	return SiteRoot(site) + "." + suffix
}

// BrokerRoot is the broker namespace root WITH a trailing dot, suitable
// for direct concatenation with leaf keys:
//   "system.<system_name>.site.<site>.docker_broker."
func BrokerRoot(site string) string {
	return SitePath(site, "docker_broker") + "."
}
