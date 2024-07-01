package flypg

import (
	"os"
	"testing"
)

func TestNewBarman(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		setDefaultEnv(t)

		configURL := os.Getenv("BARMAN_ENABLED")
		barman, err := NewBarman(configURL, "barman")
		if err != nil {
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
		if barman.minimumRedundancy != "3" {
			t.Fatalf("expected minimumRedundancy to be 3, but got %s", barman.minimumRedundancy)
		}

		if barman.retentionDays != "7" {
			t.Fatalf("expected retentionDays to be 7, but got %s", barman.retentionDays)
		}
	})
}

func TestBarmanRetentionPolicy(t *testing.T) {
	setDefaultEnv(t)
	configURL := os.Getenv("BARMAN_ENABLED")
	barman, err := NewBarman(configURL, DefaultAuthProfile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if barman.RetentionPolicy() != "'RECOVERY WINDOW OF 7 days'" {
		t.Fatalf("expected retention policy to be 'RECOVERY WINDOW OF 7 days', but got %s", barman.RetentionPolicy())
	}
}

func setDefaultEnv(t *testing.T) {
	t.Setenv("BARMAN_ENABLED", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")
	t.Setenv("FLY_APP_NAME", "postgres-flex")

}
