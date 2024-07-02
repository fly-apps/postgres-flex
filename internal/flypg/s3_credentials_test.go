package flypg

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

func TestWriteAWSCredentials(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	authDir := "./test_results/.aws"
	pathToCredentials := fmt.Sprintf("%s/credentials", authDir)

	t.Run("write-barman-credentials", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")

		if err := writeS3Credentials(context.TODO(), authDir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !utils.FileExists(pathToCredentials) {
			t.Fatalf("expected %s to exist, but doesn't", pathToCredentials)
		}

		// Check contents
		contents, err := os.ReadFile(pathToCredentials)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := "[barman]\naws_access_key_id=my-key\naws_secret_access_key=my-secret\n\n"

		if string(contents) != expected {
			t.Fatalf("expected contents to be %s, but got %s", expected, string(contents))
		}
	})

	t.Run("write-restore-credentials", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://source-key:source-secret@fly.storage.tigris.dev/my-bucket/my-directory")

		if err := writeS3Credentials(context.TODO(), authDir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !utils.FileExists(pathToCredentials) {
			t.Fatalf("expected %s to exist, but doesn't", pathToCredentials)
		}

		// Check contents
		contents, err := os.ReadFile(pathToCredentials)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := "[restore]\naws_access_key_id=source-key\naws_secret_access_key=source-secret\n\n"

		if string(contents) != expected {
			t.Fatalf("expected contents to be %s, but got %s", expected, string(contents))
		}
	})

	t.Run("write-barman-and-restore-credentials", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://source-key:source-secret@fly.storage.tigris.dev/source-bucket/source-directory")

		if err := writeS3Credentials(context.TODO(), authDir); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !utils.FileExists(pathToCredentials) {
			t.Fatalf("expected %s to exist, but doesn't", pathToCredentials)
		}

		// Check contents
		contents, err := os.ReadFile(pathToCredentials)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := "[barman]\naws_access_key_id=my-key\naws_secret_access_key=my-secret\n\n[restore]\naws_access_key_id=source-key\naws_secret_access_key=source-secret\n\n"

		if string(contents) != expected {
			t.Fatalf("expected contents to be %s, but got %s", expected, string(contents))
		}
	})

}
