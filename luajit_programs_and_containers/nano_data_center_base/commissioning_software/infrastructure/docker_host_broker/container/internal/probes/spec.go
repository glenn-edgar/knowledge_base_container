package probes

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// labelPrefix is the namespace under which dcs_host's spec_adapter
// emits per-slot probe configuration on each container's labels. See
// WIRE_PROTOCOL.md § "Broker-active HTTP probes" for the table.
const labelPrefix = "nanodatacenter.probe."

// Default values for optional probe fields. Match the catalog defaults
// documented in definitions.lua so a slot that only declares
// {path = "/health"} resolves to the same shape as one that declares
// the full block.
const (
	defaultExpectStatus = 200
	defaultIntervalS    = 5
	defaultTimeoutMS    = 2000
)

// SlotSpec is one probe target on a container. A container may have
// multiple slots (e.g. test_app's exceptions_ui + logs_ui).
type SlotSpec struct {
	Slot          string        // matches port_spec slot name
	Path          string        // URL path, e.g. "/health"
	InternalPort  int           // port the image binds inside the container
	ExpectStatus  int           // HTTP status that counts as success
	Interval      time.Duration // between probe attempts
	Timeout       time.Duration // per attempt
}

// ParseLabels walks a container's label map and returns the probe-slot
// list it declares. A slot is recognized when the labels include both
// path and internal_port for the same slot name; missing optional
// fields fall back to defaults.
//
// Errors are returned as a slice (not a single error) so the caller
// can log every malformed slot rather than stopping at the first one.
// Slots without errors are still returned even when other slots on the
// same container failed parsing.
func ParseLabels(labels map[string]string) (slots []SlotSpec, errs []error) {
	if len(labels) == 0 {
		return nil, nil
	}

	// raw[slot][field] = value
	raw := map[string]map[string]string{}
	for k, v := range labels {
		if !strings.HasPrefix(k, labelPrefix) {
			continue
		}
		rest := k[len(labelPrefix):] // "<slot>.<field>"
		dot := strings.IndexByte(rest, '.')
		if dot < 0 {
			errs = append(errs, fmt.Errorf("probe label %q: missing .<field> suffix", k))
			continue
		}
		slot, field := rest[:dot], rest[dot+1:]
		if slot == "" || field == "" {
			errs = append(errs, fmt.Errorf("probe label %q: empty slot or field", k))
			continue
		}
		if raw[slot] == nil {
			raw[slot] = map[string]string{}
		}
		raw[slot][field] = v
	}

	if len(raw) == 0 {
		return nil, errs
	}

	// stable iteration order so log output and snapshot aggregation
	// don't reshuffle on every poll
	slotNames := make([]string, 0, len(raw))
	for n := range raw {
		slotNames = append(slotNames, n)
	}
	sort.Strings(slotNames)

	for _, name := range slotNames {
		fields := raw[name]
		spec, err := buildSlot(name, fields)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		slots = append(slots, spec)
	}
	return slots, errs
}

func buildSlot(name string, fields map[string]string) (SlotSpec, error) {
	path, ok := fields["path"]
	if !ok || path == "" {
		return SlotSpec{}, fmt.Errorf("probe slot %q: missing required label .path", name)
	}
	if !strings.HasPrefix(path, "/") {
		return SlotSpec{}, fmt.Errorf("probe slot %q: .path must start with '/' (got %q)", name, path)
	}

	portStr, ok := fields["internal_port"]
	if !ok || portStr == "" {
		return SlotSpec{}, fmt.Errorf("probe slot %q: missing required label .internal_port", name)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return SlotSpec{}, fmt.Errorf("probe slot %q: .internal_port %q must be an int 1..65535", name, portStr)
	}

	expectStatus, err := parseIntDefault(name, "expect_status", fields, defaultExpectStatus, 100, 599)
	if err != nil {
		return SlotSpec{}, err
	}

	intervalS, err := parseIntDefault(name, "interval_s", fields, defaultIntervalS, 1, 3600)
	if err != nil {
		return SlotSpec{}, err
	}

	timeoutMS, err := parseIntDefault(name, "timeout_ms", fields, defaultTimeoutMS, 50, 60000)
	if err != nil {
		return SlotSpec{}, err
	}

	return SlotSpec{
		Slot:         name,
		Path:         path,
		InternalPort: port,
		ExpectStatus: expectStatus,
		Interval:     time.Duration(intervalS) * time.Second,
		Timeout:      time.Duration(timeoutMS) * time.Millisecond,
	}, nil
}

func parseIntDefault(slot, field string, fields map[string]string, def, min, max int) (int, error) {
	v, ok := fields[field]
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("probe slot %q: .%s %q must be an integer", slot, field, v)
	}
	if n < min || n > max {
		return 0, fmt.Errorf("probe slot %q: .%s %d out of range [%d..%d]", slot, field, n, min, max)
	}
	return n, nil
}
