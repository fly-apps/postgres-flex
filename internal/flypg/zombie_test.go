package flypg

import (
	"errors"
	"testing"
)

func TestZombieDiagnosis(t *testing.T) {

	t.Run("SingleMember", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  1,
			totalInactive: 0,
			totalActive:   1,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if err != nil {
			t.Fatal(err)
		}

		if primary != sample.hostname {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}

	})

	t.Run("TwoMember", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  2,
			totalInactive: 0,
			totalActive:   2,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if err != nil {
			t.Fatal(err)
		}

		if primary != sample.hostname {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}
	})

	t.Run("TwoMemberWithInactiveStandby", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  2,
			totalInactive: 1,
			totalActive:   1,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("TwoNodeWithDivergedPrimary", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  2,
			totalInactive: 1,
			totalActive:   1,
			conflictMap: map[string]int{
				"host-2": 1,
			},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("ThreeMember", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  3,
			totalInactive: 0,
			totalActive:   3,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if err != nil {
			t.Fatal(err)
		}

		if primary != sample.hostname {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}
	})

	t.Run("ThreeMemberWithOneOfflineStandby", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  3,
			totalInactive: 1,
			totalActive:   2,
			conflictMap:   map[string]int{},
		}
		primary, err := ZombieDiagnosis(sample)
		if err != nil {
			t.Fatal(err)
		}

		if primary != sample.hostname {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}
	})

	t.Run("ThreeMemberWithTwoOfflineStandby", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  3,
			totalInactive: 2,
			totalActive:   1,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("ThreeMemberWithStandbyReportDiffPrimary", func(t *testing.T) {
		sample := &DNASample{
			hostname:       "host-1",
			totalMembers:   3,
			totalInactive:  0,
			totalActive:    3,
			totalConflicts: 2,
			conflictMap: map[string]int{
				"host-99": 2,
			},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "host-99" {
			t.Fatalf("expected %s, got %q", "host-99", primary)
		}
	})

	t.Run("ThreeMemberWithOneStandbyInDisagreement", func(t *testing.T) {
		sample := &DNASample{
			hostname:       "host-1",
			totalMembers:   3,
			totalInactive:  0,
			totalActive:    3,
			totalConflicts: 1,
			conflictMap: map[string]int{
				"host-99": 1,
			},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}
	})

	t.Run("ThreeMemberWithTwoReportingDifferentPrimaries", func(t *testing.T) {
		sample := &DNASample{
			hostname:       "host-1",
			totalMembers:   3,
			totalInactive:  0,
			totalActive:    3,
			totalConflicts: 2,
			conflictMap: map[string]int{
				"host-99": 1,
				"host-33": 1,
			},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("FourMember", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  4,
			totalInactive: 0,
			totalActive:   4,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if err != nil {
			t.Fatal(err)
		}

		if primary != sample.hostname {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}
	})

	t.Run("FourMemberWithOneStandbyInactive", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  4,
			totalInactive: 1,
			totalActive:   3,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if err != nil {
			t.Fatal(err)
		}

		if primary != sample.hostname {
			t.Fatalf("expected %s, got %q", sample.hostname, primary)
		}
	})

	t.Run("FourMemberWithTwoStandbyInactive", func(t *testing.T) {
		sample := &DNASample{
			hostname:      "host-1",
			totalMembers:  4,
			totalInactive: 2,
			totalActive:   2,
			conflictMap:   map[string]int{},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("FourMemberWithTwoReportingDifferentPrimary", func(t *testing.T) {
		sample := &DNASample{
			hostname:       "host-1",
			totalMembers:   4,
			totalInactive:  0,
			totalActive:    4,
			totalConflicts: 2,
			conflictMap: map[string]int{
				"host-99": 2,
			},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiagnosisUndecided) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("FourMemberWithThreeStandbyReportingDiffPrimary", func(t *testing.T) {
		sample := &DNASample{
			hostname:       "host-1",
			totalMembers:   4,
			totalInactive:  0,
			totalActive:    4,
			totalConflicts: 3,
			conflictMap: map[string]int{
				"host-99": 3,
			},
		}

		primary, err := ZombieDiagnosis(sample)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "host-99" {
			t.Fatalf("expected %s, got %q", "host-99", primary)
		}
	})

}
