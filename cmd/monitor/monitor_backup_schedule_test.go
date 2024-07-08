package main

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

const (
	testBarmanConfigDir = "./test_results/barman"
	pgTestDirectory     = "./test_results/"
)

func TestBackupFrequency(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	setDefaultEnv(t)

	store, _ := state.NewStore()

	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := barman.LoadConfig(testBarmanConfigDir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	frequency := backupFrequency(barman)
	expected := time.Hour * 24
	if frequency != expected {
		t.Fatalf("expected frequency to be %s, but got %s", expected, frequency)
	}
}

func TestCalculateNextBackupTime(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	setDefaultEnv(t)

	store, _ := state.NewStore()
	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := barman.LoadConfig(testBarmanConfigDir); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("no backups", func(t *testing.T) {
		nextBackupTime := calculateNextBackupTime(barman, time.Time{})

		expected := 0.0
		val := math.Round(nextBackupTime.Hours())
		if expected != val {
			t.Fatalf("expected next backup time duration to be %f, but got %f", expected, val)
		}
	})

	t.Run("backup-reset", func(t *testing.T) {
		nextBackupTime := calculateNextBackupTime(barman, time.Now())

		expected := 24.0
		val := math.Round(nextBackupTime.Hours())
		if val != expected {
			t.Fatalf("expected next backup duration to be %f, but got %f", expected, val)
		}
	})

	t.Run("backup-delay", func(t *testing.T) {
		delay := time.Now().Add(-backupFrequency(barman) + time.Minute*30)

		nextBackupTime := calculateNextBackupTime(barman, delay)

		expected := 30.0
		val := math.Round(nextBackupTime.Minutes())
		if val != expected {
			t.Fatalf("expected next backup duration to be %f, but got %f", expected, val)
		}
	})

	t.Run("recent backup", func(t *testing.T) {
		lastBackup := time.Now().Add(-1 * time.Hour)

		nextBackupTime := calculateNextBackupTime(barman, lastBackup)

		expected := 23.0
		val := math.Round(nextBackupTime.Hours())
		if val != expected {
			t.Fatalf("expected next backup time duration to be %f, but got %f", expected, val)
		}
	})

	t.Run("old backup", func(t *testing.T) {
		lastBackup := time.Now().Add(-25 * time.Hour)

		nextBackupTime := calculateNextBackupTime(barman, lastBackup)

		expected := -1.0
		val := math.Round(nextBackupTime.Hours())
		if val != expected {
			t.Fatalf("expected next backup time duration to be %f, but got %f", expected, val)
		}
	})

}

func setDefaultEnv(t *testing.T) {
	t.Setenv("S3_ARCHIVE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")
	t.Setenv("FLY_APP_NAME", "postgres-flex")
}

func setup(t *testing.T) error {
	t.Setenv("FLY_VM_MEMORY_MB", fmt.Sprint(256*(1024*1024)))
	t.Setenv("UNIT_TESTING", "true")

	if _, err := os.Stat(pgTestDirectory); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(pgTestDirectory, 0750); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func cleanup() {
	if err := os.RemoveAll(pgTestDirectory); err != nil {
		fmt.Printf("failed to remove testing dir: %s\n", err)
	}
}
