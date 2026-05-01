// Package pathkb composes the DCS knowledge-base ltree path shape used
// by the broker's pg mirror (knowledge_base_status) and NATS subjects.
// Single source of truth for the namespace so Phase B Layer M phase 2
// (rename system.site.<S>.* -> system.<sys>.site.<S>.*) is a one-place
// edit. Mirrors ndc_paths.lua on the dcs_host side.
package pathkb

// SiteRoot returns the site-rooted prefix (without trailing dot):
//   "system.site.<site>"
func SiteRoot(site string) string {
	return "system.site." + site
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
//   "system.site.<site>.docker_broker."
func BrokerRoot(site string) string {
	return SitePath(site, "docker_broker") + "."
}
