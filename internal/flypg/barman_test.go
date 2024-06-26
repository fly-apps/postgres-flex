package flypg

import (
	"testing"
)

func TestNewBarman(t *testing.T) {

	t.Run("defaults", func(t *testing.T) {
		setDefaultEnv(t)

		barman, err := NewBarman()
		if err != nil {
			t.Fatal(err)
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

	t.Run("custom-retention", func(t *testing.T) {
		setDefaultEnv(t)

		t.Setenv("CLOUD_ARCHIVING_RETENTION_DAYS", "30")

		barman, err := NewBarman()
		if err != nil {
			t.Fatal(err)
		}

		if barman.retentionDays != "30" {
			t.Fatalf("expected retentionDays to be 30, but got %s", barman.retentionDays)
		}
	})

	t.Run("custom-min-redundancy", func(t *testing.T) {
		setDefaultEnv(t)
		t.Setenv("CLOUD_ARCHIVING_MINIMUM_REDUNDANCY", "7")

		barman, err := NewBarman()
		if err != nil {
			t.Fatal(err)
		}

		if barman.minimumRedundancy != "7" {
			t.Fatalf("expected retentionDays to be 7, but got %s", barman.retentionDays)
		}
	})

	t.Run("test-failure", func(t *testing.T) {
		_, err := NewBarman()
		if err == nil {
			t.Fatal("expected error, but got nil")
		}
	})
}

func TestValidateBarmanRequirements(t *testing.T) {

	t.Run("missing-aws-access-key", func(t *testing.T) {
		err := validateBarmanRequirements()

		if err.Error() != "AWS_ACCESS_KEY_ID secret must be set" {
			t.Fatalf("expected error to be 'AWS_ACCESS_KEY_ID secret must be set', but got %s", err.Error())
		}
	})

	t.Run("missing-aws-secret-access-key", func(t *testing.T) {
		setDefaultEnv(t)
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		err := validateBarmanRequirements()

		if err.Error() != "AWS_SECRET_ACCESS_KEY secret must be set" {
			t.Fatalf("expected error to be 'AWS_SECRET_ACCESS_KEY secret must be set', but got %s", err.Error())
		}
	})

	t.Run("missing-aws-bucket-name", func(t *testing.T) {
		setDefaultEnv(t)
		t.Setenv("AWS_BUCKET_NAME", "")
		err := validateBarmanRequirements()

		if err.Error() != "AWS_BUCKET_NAME envvar must be set" {
			t.Fatalf("expected error to be 'AWS_BUCKET_NAME envvar must be set', but got %s", err.Error())
		}
	})

	t.Run("missing-aws-endpoint-url", func(t *testing.T) {
		setDefaultEnv(t)
		t.Setenv("AWS_ENDPOINT_URL", "")
		err := validateBarmanRequirements()

		if err.Error() != "AWS_ENDPOINT_URL envvar must be set" {
			t.Fatalf("expected error to be 'AWS_ENDPOINT_URL envvar must be set', but got %s", err.Error())
		}
	})
}

func TestBarmanRetentionPolicy(t *testing.T) {
	setDefaultEnv(t)

	barman, err := NewBarman()
	if err != nil {
		t.Fatal(err)
	}

	if barman.RetentionPolicy() != "'RECOVERY WINDOW OF 7 days'" {
		t.Fatalf("expected retention policy to be 'RECOVERY WINDOW OF 7 days', but got %s", barman.RetentionPolicy())
	}

}

func setDefaultEnv(t *testing.T) {
	t.Setenv("CLOUD_ARCHIVING_ENABLED", "true")
	t.Setenv("FLY_APP_NAME", "postgres-flex")
	t.Setenv("AWS_ACCESS_KEY_ID", "my-key")
	t.Setenv("AWS_ENDPOINT_URL", "https://fly.storage.tigris.dev")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "my-secret")
	t.Setenv("AWS_BUCKET_NAME", "my-bucket")
}
