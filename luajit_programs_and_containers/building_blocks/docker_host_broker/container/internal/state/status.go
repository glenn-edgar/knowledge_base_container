package state

import (
	"sync/atomic"
	"time"
)

// Status holds broker-wide runtime state shared between natspub and
// httpapi. All fields are atomically accessible.
type Status struct {
	startTime        time.Time
	dockerSocketOK   atomic.Bool
	heartbeatSeq     atomic.Uint64
	brokerVersion    string
}

// NewStatus returns a Status with start time set to now.
func NewStatus(brokerVersion string) *Status {
	s := &Status{
		startTime:     time.Now(),
		brokerVersion: brokerVersion,
	}
	s.dockerSocketOK.Store(false)
	return s
}

// SetDockerSocketOK is called by the container poll loop to flip the
// flag whenever a docker daemon call succeeds or fails.
func (s *Status) SetDockerSocketOK(ok bool) { s.dockerSocketOK.Store(ok) }

// DockerSocketOK is read by heartbeat publishes and the /v1/health endpoint.
func (s *Status) DockerSocketOK() bool { return s.dockerSocketOK.Load() }

// IncHeartbeatSeq atomically increments and returns the new sequence.
func (s *Status) IncHeartbeatSeq() uint64 { return s.heartbeatSeq.Add(1) }

// HeartbeatSeq returns the current heartbeat sequence without incrementing.
func (s *Status) HeartbeatSeq() uint64 { return s.heartbeatSeq.Load() }

// Uptime returns time since broker start.
func (s *Status) Uptime() time.Duration { return time.Since(s.startTime) }

// BrokerVersion returns the configured version string.
func (s *Status) BrokerVersion() string { return s.brokerVersion }
