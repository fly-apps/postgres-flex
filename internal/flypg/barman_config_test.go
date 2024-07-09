package flypg

import (
	"testing"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

func TestValidateBarmanConfig(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	store, _ := state.NewStore()

	b, err := NewBarmanConfig(store, testBarmanConfigDir)
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

	t.Run("valid-archive-timeout-us", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120us"}

		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("archive-timeout-with-ms-unit", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120ms"}

		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("archive-timeout-with-s-unit", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120s"}

		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("archive-timeout-with-m-unit", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120m"}

		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		conf = ConfigMap{"archive_timeout": "120min"}
		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("archive-timeout-with-h-unit", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120h"}
		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("archive-timeout-with-d-unit", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120d"}
		if err := b.Validate(conf); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("invalid-archive-timeout", func(t *testing.T) {
		conf := ConfigMap{"archive_timeout": "120seconds"}
		err := b.Validate(conf)
		if err == nil {
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

		conf = ConfigMap{
			"recovery_window": "1m",
		}

		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected error, got nil")
		}

		conf = ConfigMap{
			"recovery_window": "0w",
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

		conf = ConfigMap{
			"full_backup_frequency": "0w",
		}
		if err := b.Validate(conf); err == nil {
			t.Fatalf("expected invalid value for recovery_window (expected to be >= 1, got, got 1")
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
		b, err := NewBarmanConfig(store, testBarmanConfigDir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if b.Settings.MinimumRedundancy != "3" {
			t.Fatalf("expected minimumRedundancy to be 3, but got %s", b.Settings.MinimumRedundancy)
		}

		if b.Settings.FullBackupFrequency != "24h" {
			t.Fatalf("expected fullBackupFrequency to be 24h, but got %s", b.Settings.FullBackupFrequency)
		}

		if b.Settings.RecoveryWindow != "RECOVERY WINDOW OF 7 DAYS" {
			t.Fatalf("expected recovery_window to be 'RECOVERY WINDOW OF 7 DAYS', but got %s", b.Settings.RecoveryWindow)
		}

		if b.Settings.ArchiveTimeout != "60s" {
			t.Fatalf("expected archive_timeout to be 60s, but got %s", b.Settings.ArchiveTimeout)
		}

	})
}

func TestBarmanSettingUpdate(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	store, _ := state.NewStore()

	b, err := NewBarmanConfig(store, testBarmanConfigDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	usrCfg := ConfigMap{
		"archive_timeout": "60m",
	}

	if err := b.Validate(usrCfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b.SetUserConfig(usrCfg)

	if err := writeUserConfigFile(b); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg, err := b.CurrentConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg["archive_timeout"] != "60m" {
		t.Fatalf("expected archive_timeout to be 60m, but got %s", cfg["archive_timeout"])
	}
}
