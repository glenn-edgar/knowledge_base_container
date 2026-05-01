// Package hoststats reads /proc, /sys, and df to populate
// state.HostMetrics.
//
// Reads are bounded — local-fs only, no shell-outs. Each sampler is a
// few microseconds. /proc/stat for CPU, /proc/meminfo for memory,
// /proc/loadavg for load, /proc/net/dev for net rates, statvfs for
// disk used %.
//
// PHASE 1 SCAFFOLD: types and method signatures only. Bodies TODO.
package hoststats

import (
	"errors"

	"github.com/nanodatacenter/docker_host_broker/internal/state"
)

// Reader samples /proc-derived host metrics.
type Reader interface {
	// Sample returns a fresh HostMetrics snapshot. Net rates are deltas
	// against the previous Sample call.
	Sample() (state.HostMetrics, error)
	Close() error
}

// ErrNotImplemented is returned by every method in the scaffold.
var ErrNotImplemented = errors.New("hoststats: not implemented in scaffold")

// New constructs a Reader rooted at procRoot (typically /proc, or
// /host/proc when running in a container with /proc bind-mounted).
func New(procRoot string) (Reader, error) {
	return nil, ErrNotImplemented
}
