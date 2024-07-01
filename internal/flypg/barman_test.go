package flypg

import (
	"fmt"
	"os"
	"testing"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

func TestNewBarman(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		setDefaultEnv(t)

		barman, err := NewBarman(os.Getenv("BARMAN_ENABLED"))
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

func TestWALRestoreCommand(t *testing.T) {
	setDefaultEnv(t)

	barman, err := NewBarman(os.Getenv("BARMAN_ENABLED"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := fmt.Sprintf("barman-cloud-wal-restore --cloud-provider aws-s3 --endpoint-url https://fly.storage.tigris.dev s3://my-bucket my-directory %%f %%p")

	if barman.walRestoreCommand() != expected {
		t.Fatalf("expected WALRestoreCommand to be %s, but got %s", expected, barman.walRestoreCommand())
	}
}

func TestWriteAWSCredentials(t *testing.T) {
	setup(t)
	defer cleanup()

	t.Run("write-aws-credentials", func(t *testing.T) {
		setDefaultEnv(t)

		barman, err := NewBarman(os.Getenv("BARMAN_ENABLED"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		credFile := "./test_results/credentials"

		if err := barman.writeAWSCredentials("default", credFile); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !utils.FileExists(credFile) {
			t.Fatalf("expected %s to exist, but doesn't", credFile)
		}

		// Check contents
		contents, err := os.ReadFile(credFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := "[default]\naws_access_key_id=my-key\naws_secret_access_key=my-secret"

		if string(contents) != expected {
			t.Fatalf("expected contents to be %s, but got %s", expected, string(contents))
		}
	})
}

func TestBarmanRetentionPolicy(t *testing.T) {
	setDefaultEnv(t)

	barman, err := NewBarman(os.Getenv("BARMAN_ENABLED"))
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
