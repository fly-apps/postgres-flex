package flypg

import (
	"testing"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

const (
	barmanConfigTestDir = "./test_results/barman"
)

func TestValidateBarmanConfig(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	store, _ := state.NewStore()

	b, err := NewBarmanConfig(store, barmanConfigTestDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("valid-config", func(t *testing.T) {

		conf := ConfigMap{
			"archive_timeout":       "120s",
			"recovery_window":       "7d",
			"full_backup_frequency": "24h",
			"minimum_redundancy":    "3",
		}

		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid-archive-timeout", func(t *testing.T) {
		conf := ConfigMap{
			"archive_timeout": "120seconds",
		}

		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("invalid-recovery-window", func(t *testing.T) {
		conf := ConfigMap{
			"recovery_window": "10seconds",
		}

		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("invalid-full-backup-frequency", func(t *testing.T) {
		conf := ConfigMap{
			"full_backup_frequency": "10seconds",
		}

		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected error, got nil")
		}

		conf = ConfigMap{
			"full_backup_frequency": "1m",
		}

		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("invalid-minimum-redundancy", func(t *testing.T) {
		conf := ConfigMap{
			"minimum_redundancy": "-1",
		}

		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

func TestBarmanConfigSettings(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	store, _ := state.NewStore()

	t.Run("defaults", func(t *testing.T) {
		b, err := NewBarmanConfig(store, barmanConfigTestDir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if b.Settings.MinimumRedundancy != "3" {
			t.Fatalf("expected minimumRedundancy to be 3, but got %s", b.Settings.MinimumRedundancy)
		}

		if b.Settings.FullBackupFrequency != "24h" {
			t.Fatalf("expected fullBackupFrequency to be 24h, but got %s", b.Settings.FullBackupFrequency)
		}

	})
}
