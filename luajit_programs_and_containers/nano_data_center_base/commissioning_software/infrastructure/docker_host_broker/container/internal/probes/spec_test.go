package probes

import (
	"testing"
	"time"
)

func TestParseLabels_NoProbeLabels(t *testing.T) {
	slots, errs := ParseLabels(map[string]string{
		"nanodatacenter":     "true",
		"some.other.label":   "x",
	})
	if len(slots) != 0 || len(errs) != 0 {
		t.Fatalf("expected no slots and no errors, got slots=%v errs=%v", slots, errs)
	}
}

func TestParseLabels_FullSlotAllDefaults(t *testing.T) {
	slots, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.exceptions_ui.path":          "/health",
		"nanodatacenter.probe.exceptions_ui.internal_port": "8080",
	})
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(slots) != 1 {
		t.Fatalf("expected 1 slot, got %d", len(slots))
	}
	s := slots[0]
	if s.Slot != "exceptions_ui" || s.Path != "/health" || s.InternalPort != 8080 {
		t.Fatalf("required fields wrong: %+v", s)
	}
	if s.ExpectStatus != defaultExpectStatus {
		t.Fatalf("expect_status default lost: got %d", s.ExpectStatus)
	}
	if s.Interval != time.Duration(defaultIntervalS)*time.Second {
		t.Fatalf("interval default lost: got %v", s.Interval)
	}
	if s.Timeout != time.Duration(defaultTimeoutMS)*time.Millisecond {
		t.Fatalf("timeout default lost: got %v", s.Timeout)
	}
}

func TestParseLabels_FullSlotExplicit(t *testing.T) {
	slots, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.logs_ui.path":          "/api/healthz",
		"nanodatacenter.probe.logs_ui.internal_port": "8081",
		"nanodatacenter.probe.logs_ui.expect_status": "204",
		"nanodatacenter.probe.logs_ui.interval_s":    "10",
		"nanodatacenter.probe.logs_ui.timeout_ms":    "500",
	})
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(slots) != 1 {
		t.Fatalf("expected 1 slot, got %d", len(slots))
	}
	s := slots[0]
	if s.Path != "/api/healthz" || s.InternalPort != 8081 ||
		s.ExpectStatus != 204 || s.Interval != 10*time.Second ||
		s.Timeout != 500*time.Millisecond {
		t.Fatalf("explicit values not preserved: %+v", s)
	}
}

func TestParseLabels_MultipleSlotsSortedByName(t *testing.T) {
	slots, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.zebra.path":          "/z",
		"nanodatacenter.probe.zebra.internal_port": "9001",
		"nanodatacenter.probe.alpha.path":          "/a",
		"nanodatacenter.probe.alpha.internal_port": "9000",
	})
	if len(errs) != 0 {
		t.Fatalf("unexpected errs: %v", errs)
	}
	if len(slots) != 2 {
		t.Fatalf("expected 2 slots, got %d", len(slots))
	}
	if slots[0].Slot != "alpha" || slots[1].Slot != "zebra" {
		t.Fatalf("slots not sorted: %v", []string{slots[0].Slot, slots[1].Slot})
	}
}

func TestParseLabels_MissingPathErrors(t *testing.T) {
	slots, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.x.internal_port": "8080",
	})
	if len(slots) != 0 {
		t.Fatalf("slot should not be returned when path missing: %v", slots)
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d: %v", len(errs), errs)
	}
}

func TestParseLabels_BadPath(t *testing.T) {
	_, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.x.path":          "health", // missing leading /
		"nanodatacenter.probe.x.internal_port": "8080",
	})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for bad path, got %v", errs)
	}
}

func TestParseLabels_BadPort(t *testing.T) {
	_, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.x.path":          "/h",
		"nanodatacenter.probe.x.internal_port": "70000", // out of range
	})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for bad port, got %v", errs)
	}
}

func TestParseLabels_BadIntervalDoesNotKillSiblingSlot(t *testing.T) {
	slots, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.bad.path":          "/h",
		"nanodatacenter.probe.bad.internal_port": "8080",
		"nanodatacenter.probe.bad.interval_s":    "abc",

		"nanodatacenter.probe.good.path":          "/h",
		"nanodatacenter.probe.good.internal_port": "8081",
	})
	if len(slots) != 1 || slots[0].Slot != "good" {
		t.Fatalf("good slot should survive bad sibling: %v", slots)
	}
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for bad slot, got %v", errs)
	}
}

func TestParseLabels_MalformedKey(t *testing.T) {
	_, errs := ParseLabels(map[string]string{
		"nanodatacenter.probe.no_field_suffix": "x", // no .<field>
	})
	if len(errs) != 1 {
		t.Fatalf("expected 1 error for malformed key, got %v", errs)
	}
}
