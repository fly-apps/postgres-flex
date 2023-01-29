package flypg

import (
	"errors"
	"testing"
)

func TestZombieDiagnosis(t *testing.T) {

	t.Run("single node cluster", func(t *testing.T) {
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

	t.Run("two node cluster", func(t *testing.T) {
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

	t.Run("two node cluster with inactive standby", func(t *testing.T) {
		hostname := "host-1"
		total := 2
		inactive := 1
		active := 1

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, map[string]int{})
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("two node cluster with diverged primary", func(t *testing.T) {
		hostname := "host-1"
		total := 2
		inactive := 1
		active := 1
		conflictMap := map[string]int{
			"host-2": 1,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("three node cluster", func(t *testing.T) {
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

	t.Run("three node cluster with one offline standby", func(t *testing.T) {
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

	t.Run("three node cluster with both standbys offline", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 2
		active := 1
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("three node cluster with real primary resolved", func(t *testing.T) {
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

	t.Run("three node cluster with one standby in disagreement", func(t *testing.T) {
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

	t.Run("three node cluster with two standbys reporting different primarys", func(t *testing.T) {
		hostname := "host-1"
		total := 3
		inactive := 0
		active := 3
		conflictMap := map[string]int{
			"host-99": 1,
			"host-33": 1,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("four node setup", func(t *testing.T) {
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

	t.Run("four node setup with one standby inactive", func(t *testing.T) {
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

	t.Run("four node setup with two standbys inactive", func(t *testing.T) {
		hostname := "host-1"
		total := 4
		inactive := 2
		active := 2
		conflictMap := map[string]int{}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("four node setup with two standbys reporting different primary", func(t *testing.T) {
		hostname := "host-1"

		total := 4
		inactive := 0
		active := 4
		conflictMap := map[string]int{
			"host-99": 2,
		}

		primary, err := ZombieDiagnosis(hostname, total, inactive, active, conflictMap)
		if !errors.Is(err, ErrZombieDiscovered) {
			t.Fatal(err)
		}

		if primary != "" {
			t.Fatalf("expected %s, got %q", "", primary)
		}
	})

	t.Run("four node setup with three standbys reporting different primary", func(t *testing.T) {
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
