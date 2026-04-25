// Package probes implements broker-active HTTP health probes against
// container internal IPs. See WIRE_PROTOCOL.md § "Broker-active HTTP
// probes (Phase 4)" for the wire contract.
package probes

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
)

// RouteNoRoute is the sentinel value used in the wire-protocol
// `probe.route` field when the broker shares no network with the
// container. Supervisors MUST treat as skip, never as failure.
const RouteNoRoute = "no_route"

// Router picks the right IP to probe for a given container, based on
// the set of networks the broker itself sits on. Resolved once at
// broker startup; if the broker's networks change at runtime, restart
// the broker.
//
// The pick rule mirrors WIRE_PROTOCOL.md § Q2 (Phase 4 design): prefer
// any shared non-`bridge` network (e.g. planner-net) over the default
// bridge. This way internal app traffic prefers the dedicated network
// when both are available.
type Router struct {
	// networks is the set of network names the broker is connected to.
	networks map[string]struct{}
}

// NewRouter inspects the broker's own container, caches its set of
// networks, and returns a Router. The hostname passed to InspectContainer
// comes from os.Hostname() — inside a docker container this is the
// container's short ID by default, which InspectContainer accepts.
//
// If the broker is run outside a container (host-network or for local
// testing without docker) the inspect will fail; callers should treat
// that as a fatal config error since probes can't function without it.
func NewRouter(ctx context.Context, dock dockercli.Client) (*Router, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("probes.NewRouter: os.Hostname: %w", err)
	}
	ci, err := dock.InspectContainer(ctx, hostname)
	if err != nil {
		return nil, fmt.Errorf("probes.NewRouter: InspectContainer(%q): %w", hostname, err)
	}
	nets := make(map[string]struct{}, len(ci.IPAddresses))
	for n := range ci.IPAddresses {
		nets[n] = struct{}{}
	}
	return &Router{networks: nets}, nil
}

// Networks returns the set of network names the broker is on, sorted
// for stable logging.
func (r *Router) Networks() []string {
	out := make([]string, 0, len(r.networks))
	for n := range r.networks {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// PickIP returns (ip, network) for the container. If the broker and
// container share no network, returns ("", RouteNoRoute). The caller
// must record the route on the probe-state regardless — supervisors
// look at the route field to distinguish "skip" from "fail".
//
// When multiple shared networks exist, non-`bridge` wins. Tie-breaking
// among non-bridge networks is by sorted name (stable, predictable).
func (r *Router) PickIP(ci dockercli.ContainerInfo) (ip, network string) {
	var bridgeIP string
	var nonBridgeCandidates []string
	for n := range ci.IPAddresses {
		if _, shared := r.networks[n]; !shared {
			continue
		}
		if n == "bridge" {
			bridgeIP = ci.IPAddresses[n]
			continue
		}
		nonBridgeCandidates = append(nonBridgeCandidates, n)
	}
	if len(nonBridgeCandidates) > 0 {
		sort.Strings(nonBridgeCandidates)
		pick := nonBridgeCandidates[0]
		return ci.IPAddresses[pick], pick
	}
	if bridgeIP != "" {
		return bridgeIP, "bridge"
	}
	return "", RouteNoRoute
}
