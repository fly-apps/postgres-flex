package flypg

import (
	"errors"
	"testing"
)

func TestZombieDiagnosis(t *testing.T) {

	t.Run("SingleMember", func(t *testing.T) {
		hostname := "host-1"

		total := 1
		inactive := 0
		active := 1

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, map[string]int{})
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}

	})

	t.Run("TwoMember", func(t *testing.T) {
		hostname := "host-1"
		total := 2
		inactive := 0
		active := 2

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, map[string]int{})
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}

	})

	t.Run("TwoMemberWithInactiveStandby", func(t *testing.T) {
		hostname := "host-1"
		total := 2
		inactive := 1
		active := 1

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, map[string]int{})
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("TwoNodeWithDivergedPrimary", func(t *testing.T) {
		hostname := "host-1"
		total := 2
		inactive := 1
		active := 1
		conflictMap := map[string]int{
			"host-2": 1,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("ThreeMember", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 0
		active := 3
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}
	})

	t.Run("ThreeMemberWithOneOfflineStandby", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 1
		active := 2
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}
	})

	t.Run("ThreeMemberWithTwoOfflineStandby", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 2
		active := 1
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("ThreeMemberWithStandbyReportDiffPrimary", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 0
		active := 3
		conflictMap := map[string]int{
			"host-99": 2,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "host-99" {
			t.Fatalf("expected %s, got %q", "host-99", primary)
		}
	})

	t.Run("ThreeMemberWithOneStandbyInDisagreement", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 0
		active := 3
		conflictMap := map[string]int{
			"host-99": 1,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}
	})

	t.Run("ThreeMemberWithTwoReportingDifferentPrimaries", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 0
		active := 3
		conflictMap := map[string]int{
			"host-99": 1,
			"host-33": 1,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("FourMember", func(t *testing.T) {
		hostname := "host-1"

		total := 4
		inactive := 0
		active := 4
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}
	})

	t.Run("FourMemberWithOneStandbyInactive", func(t *testing.T) {
		hostname := "host-1"

		total := 4
		inactive := 1
		active := 3
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if err != nil {
			t.Fatal(err)
		}

		if primary != hostname {
			t.Fatalf("expected %s, got %q", hostname, primary)
		}
	})

	t.Run("FourMemberWithTwoStandbyInactive", func(t *testing.T) {
		hostname := "host-1"
		total := 4
		inactive := 2
		active := 2
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("FourMemberWithTwoReportingDifferentPrimary", func(t *testing.T) {
		hostname := "host-1"

		total := 4
		inactive := 0
		active := 4
		conflictMap := map[string]int{
			"host-99": 2,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("FourMemberWithThreeStandbyReportingDiffPrimary", func(t *testing.T) {
		hostname := "host-1"
		expected := "host-99"

		total := 4
		inactive := 0
		active := 4
		conflictMap := map[string]int{
			expected: 3,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != expected {
			t.Fatalf("expected %s, got %q", expected, primary)
		}
	})

}
