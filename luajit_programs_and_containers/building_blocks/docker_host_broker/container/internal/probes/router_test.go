package probes

import (
	"testing"

	"github.com/nanodatacenter/docker_host_broker/internal/dockercli"
)

func newRouterFromSet(nets ...string) *Router {
	m := map[string]struct{}{}
	for _, n := range nets {
		m[n] = struct{}{}
	}
	return &Router{networks: m}
}

func TestPickIP_PrefersNonBridge(t *testing.T) {
	r := newRouterFromSet("bridge", "planner-net")
	ci := dockercli.ContainerInfo{
		IPAddresses: map[string]string{
			"bridge":      "172.17.0.5",
			"planner-net": "172.20.0.4",
		},
	}
	ip, net := r.PickIP(ci)
	if ip != "172.20.0.4" || net != "planner-net" {
		t.Fatalf("expected planner-net/172.20.0.4, got %s/%s", net, ip)
	}
}

func TestPickIP_FallsBackToBridge(t *testing.T) {
	r := newRouterFromSet("bridge")
	ci := dockercli.ContainerInfo{
		IPAddresses: map[string]string{
			"bridge":      "172.17.0.5",
			"planner-net": "172.20.0.4", // broker not on this network
		},
	}
	ip, net := r.PickIP(ci)
	if ip != "172.17.0.5" || net != "bridge" {
		t.Fatalf("expected bridge/172.17.0.5, got %s/%s", net, ip)
	}
}

func TestPickIP_NoSharedNetwork_ReturnsNoRoute(t *testing.T) {
	r := newRouterFromSet("bridge")
	ci := dockercli.ContainerInfo{
		IPAddresses: map[string]string{
			"planner-net": "172.20.0.4",
		},
	}
	ip, net := r.PickIP(ci)
	if ip != "" || net != RouteNoRoute {
		t.Fatalf("expected /no_route, got %s/%s", net, ip)
	}
}

func TestPickIP_TieBreakDeterministic(t *testing.T) {
	// Two non-bridge shared networks — sorted-name pick wins.
	r := newRouterFromSet("alpha-net", "zebra-net")
	ci := dockercli.ContainerInfo{
		IPAddresses: map[string]string{
			"alpha-net": "10.0.0.5",
			"zebra-net": "10.9.0.5",
		},
	}
	ip, net := r.PickIP(ci)
	if net != "alpha-net" || ip != "10.0.0.5" {
		t.Fatalf("expected alpha-net (sorted first), got %s/%s", net, ip)
	}
}

func TestPickIP_EmptyContainerNetworks(t *testing.T) {
	r := newRouterFromSet("bridge")
	ci := dockercli.ContainerInfo{IPAddresses: map[string]string{}}
	ip, net := r.PickIP(ci)
	if ip != "" || net != RouteNoRoute {
		t.Fatalf("expected /no_route on empty IPAddresses, got %s/%s", net, ip)
	}
}
