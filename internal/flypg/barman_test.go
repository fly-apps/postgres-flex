package flypg

import (
	"os"
	"testing"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

const (
	testBarmanConfigDir = "./test_results/barman/"
)

func TestNewBarman(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	store, _ := state.NewStore()

	t.Run("defaults", func(t *testing.T) {
		setDefaultEnv(t)

		configURL := os.Getenv("S3_ARCHIVE_CONFIG")
		barman, err := NewBarman(store, configURL, DefaultAuthProfile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if err := barman.LoadConfig(testBarmanConfigDir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if barman.provider != "aws-s3" {
			t.Fatalf("expected provider to be aws, but got %s", barman.provider)
		}

		if barman.endpoint != "https://fly.storage.tigris.dev" {
			t.Fatalf("expected endpoint to be https://fly.storage.tigris.dev, but got %s", barman.endpoint)
		}

		if barman.bucket != "my-bucket" {
			t.Fatalf("expected bucket to be my-bucket, but got %s", barman.bucket)
		}

		if barman.BucketURL() != "s3://my-bucket" {
			t.Fatalf("expected bucket to be s3://my-bucket, but got %s", barman.bucket)
		}

		if barman.bucketDirectory != "my-directory" {
			t.Fatalf("expected directory to be my-directory, but got %s", barman.bucketDirectory)
		}

		if barman.appName != "postgres-flex" {
			t.Fatalf("expected appName to be postgres-flex, but got %s", barman.appName)
		}

		// Defaults
		if barman.Settings.MinimumRedundancy != "3" {
			t.Fatalf("expected minimumRedundancy to be 3, but got %s", barman.Settings.MinimumRedundancy)
		}

		if barman.Settings.RecoveryWindow != "RECOVERY WINDOW OF 7 DAYS" {
			t.Fatalf("expected recovery_window to be 'RECOVERY WINDOW OF 7 DAYS', but got %s", barman.Settings.RecoveryWindow)
		}

		if barman.Settings.FullBackupFrequency != "24h" {
			t.Fatalf("expected fullBackupFrequency to be 24, but got %s", barman.Settings.FullBackupFrequency)
		}

		if barman.Settings.ArchiveTimeout != "60s" {
			t.Fatalf("expected archiveTimeout to be 60s, but got %s", barman.Settings.ArchiveTimeout)
		}
	})
}

func TestFormatTimestamp(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		ts, err := formatTimestamp("2024-07-03T17:55:22Z")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ts != "2024-07-03T17:55:22+00:00" {
			t.Fatalf("expected timestamp to be 2024-07-03T17:55:22+00:00, but got %s", ts)
		}

		ts, err = formatTimestamp("2024-07-03T17:55:22-07:00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ts != "2024-07-03T17:55:22-07:00" {
			t.Fatalf("expected timestamp to be 2024-07-03T17:55:22-07:00, but got %s", ts)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := formatTimestamp("2024-07-03T17:55:22Z07:00")
		if err == nil {
			t.Fatalf("unexpected error, but not nil")
		}
	})
}

func setDefaultEnv(t *testing.T) {
	t.Setenv("S3_ARCHIVE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")
	t.Setenv("FLY_APP_NAME", "postgres-flex")

}
