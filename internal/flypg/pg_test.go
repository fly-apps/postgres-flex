package flypg

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	pgTestDirectory          = "./test_results"
	pgConfigFilePath         = "./test_results/postgresql.conf"
	pgInternalConfigFilePath = "./test_results/postgresql.internal.conf"
	pgUserConfigFilePath     = "./test_results/postgresql.user.conf"
	pgPasswordFilePath       = "./test_results/default_password"
	pgHBAFilePath            = "./test_results/pg_hba.conf"
)

func TestPGConfigInitialization(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		DataDir:                pgTestDirectory,
		Port:                   5433,
		ConfigFilePath:         pgConfigFilePath,
		InternalConfigFilePath: pgInternalConfigFilePath,
		UserConfigFilePath:     pgUserConfigFilePath,
		passwordFilePath:       pgPasswordFilePath,
		barmanConfigPath:       testBarmanConfigDir,
	}

	if err := stubPGConfigFile(); err != nil {
		t.Fatal(err)
	}

	t.Run("initialize", func(t *testing.T) {
		store, _ := state.NewStore()
		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("postgresql.conf", func(t *testing.T) {
		contents, err := os.ReadFile(pgConf.ConfigFilePath)
		if err != nil {
			t.Error(err)
		}

		if !strings.Contains(string(contents), "postgresql.internal.conf") {
			t.Fatalf("expected postgresql.conf to include postgresql.internal.conf")
		}

		if !strings.Contains(string(contents), "postgresql.user.conf") {
			t.Fatalf("expected postgresql.conf to include postgresql.user.conf")
		}
	})

	t.Run("configFiles", func(t *testing.T) {
		if !utils.FileExists(pgConf.InternalConfigFilePath) {
			t.Fatalf("expected %s to exist, but doesn't", pgConf.InternalConfigFilePath)
		}

		if !utils.FileExists(pgConf.UserConfigFilePath) {
			t.Fatalf("expected %s to exist, but doesn't", pgConf.UserConfigFilePath)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["port"] != "5433" {
			t.Fatalf("expected port to be 5433, got %v", cfg["port"])
		}

		if cfg["max_connections"] != "300" {
			t.Fatalf("expected max_connections to be 300, got %v", cfg["max_connections"])
		}

		if cfg["hot_standby"] != "true" {
			t.Fatalf("expected hot_standby to be true, got %v", cfg["hot_standby"])
		}

		if cfg["shared_preload_libraries"] != "'repmgr'" {
			t.Fatalf("expected 'repmgr', got %s", cfg["shared_preload_libraries"])
		}
	})

	t.Run("timescaledb", func(t *testing.T) {
		t.Setenv("TIMESCALEDB_ENABLED", "true")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		expected := "'repmgr,timescaledb'"

		if cfg["shared_preload_libraries"] != expected {
			t.Fatalf("expected %s, got %s", expected, cfg["shared_preload_libraries"])
		}
	})

	t.Run("archive-enabled", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")

		store, _ := state.NewStore()

		barman, err := NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), DefaultAuthProfile)
		if err != nil {
			t.Fatal(err)
		}

		if err := barman.LoadConfig(testBarmanConfigDir); err != nil {
			t.Fatal(err)
		}

		t.Run("defaults", func(t *testing.T) {
			if err := pgConf.initialize(store); err != nil {
				t.Fatal(err)
			}

			cfg, err := pgConf.CurrentConfig()
			if err != nil {
				t.Fatal(err)
			}

			if cfg["archive_mode"] != "on" {
				t.Fatalf("expected archive_mode to be on, got %v", cfg["archive_mode"])
			}

			expected := fmt.Sprintf("'%s'", barman.walArchiveCommand())
			if cfg["archive_command"] != expected {
				t.Fatalf("expected %s, got %s", expected, cfg["archive_command"])
			}

			if cfg["archive_timeout"] != "60s" {
				t.Fatalf("expected 60s, got %s", cfg["archive_timeout"])
			}
		})

		t.Run("custom-archive-timeout-with-m", func(t *testing.T) {

			barman.SetUserConfig(ConfigMap{"archive_timeout": "60m"})

			if err := writeUserConfigFile(barman); err != nil {
				t.Fatal(err)
			}

			if err := pgConf.initialize(store); err != nil {
				t.Fatal(err)
			}

			cfg, err := pgConf.CurrentConfig()
			if err != nil {
				t.Fatal(err)
			}

			if cfg["archive_timeout"] != "60min" {
				t.Fatalf("expected 60min, got %s", cfg["archive_timeout"])
			}
		})

		t.Run("custom-archive-timeout-with-min", func(t *testing.T) {
			barman.SetUserConfig(ConfigMap{"archive_timeout": "60min"})

			if err := writeUserConfigFile(barman); err != nil {
				t.Fatal(err)
			}

			if err := pgConf.initialize(store); err != nil {
				t.Fatal(err)
			}

			cfg, err := pgConf.CurrentConfig()
			if err != nil {
				t.Fatal(err)
			}

			if cfg["archive_timeout"] != "60min" {
				t.Fatalf("expected 60min, got %s", cfg["archive_timeout"])
			}
		})

		t.Run("custom-archive-timeout-with-s", func(t *testing.T) {
			barman.SetUserConfig(ConfigMap{"archive_timeout": "60s"})

			if err := writeUserConfigFile(barman); err != nil {
				t.Fatal(err)
			}

			if err := pgConf.initialize(store); err != nil {
				t.Fatal(err)
			}

			cfg, err := pgConf.CurrentConfig()
			if err != nil {
				t.Fatal(err)
			}

			if cfg["archive_timeout"] != "60s" {
				t.Fatalf("expected 60s, got %s", cfg["archive_timeout"])
			}
		})

		t.Run("custom-archive-timeout-w", func(t *testing.T) {
			barman.SetUserConfig(ConfigMap{"archive_timeout": "24h"})

			if err := writeUserConfigFile(barman); err != nil {
				t.Fatal(err)
			}

			if err := pgConf.initialize(store); err != nil {
				t.Fatal(err)
			}

			cfg, err := pgConf.CurrentConfig()
			if err != nil {
				t.Fatal(err)
			}

			if cfg["archive_timeout"] != "24h" {
				t.Fatalf("expected 24h, got %s", cfg["archive_timeout"])
			}
		})
	})

	t.Run("barman-disabled", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["archive_mode"] != "on" {
			t.Fatalf("expected archive_mode to be on, got %v", cfg["archive_mode"])
		}

		t.Setenv("S3_ARCHIVE_CONFIG", "")

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err = pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["archive_mode"] != "off" {
			t.Fatalf("expected archive_mode to be off, got %v", cfg["archive_mode"])
		}
	})

	t.Run("barman-restore-from-time", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetTime=2024-06-30T11:15:00-06:00")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["recovery_target_time"] != "'2024-06-30T11:15:00-06:00'" {
			t.Fatalf("expected recovery_target_time to be 2024-06-30T11:15:00-06:00, got %v", cfg["recovery_target_time"])
		}
	})

	t.Run("barman-restore-from-name", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetName=20240626T172443")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["recovery_target_name"] != "barman_20240626T172443" {
			t.Fatalf("expected recovery_target_name to be barman_20240626T172443, got %v", cfg["recovery_target_name"])
		}
	})

	t.Run("barman-restore-from-target", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?target=immediate")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["recovery_target"] != "immediate" {
			t.Fatalf("expected recovery_target to be immediate, got %v", cfg["recovery_target_name"])
		}
	})

	t.Run("barman-restore-from-target-time-non-inclusive", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetTime=2024-06-30T11:15:00Z&targetInclusive=false")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["recovery_target_time"] != "'2024-06-30T11:15:00+00:00'" {
			t.Fatalf("expected recovery_target_time to be 2024-06-30T11:15:00+00:00, got %v", cfg["recovery_target_time"])
		}

		if cfg["recovery_target_inclusive"] != "false" {
			t.Fatalf("expected recovery_target_inclusive to be false, got %v", cfg["recovery_target_inclusive"])
		}
	})

	t.Run("barman-restore-from-target-time-custom-timeline", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetTime=2024-06-30T11:15:00-06:00&targetTimeline=2")
		store, _ := state.NewStore()

		if err := pgConf.initialize(store); err != nil {
			t.Fatal(err)
		}

		cfg, err := pgConf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["recovery_target_time"] != "'2024-06-30T11:15:00-06:00'" {
			t.Fatalf("expected recovery_target_time to be 2024-06-30T11:15:00-06:00, got %v", cfg["recovery_target_time"])
		}

		if cfg["recovery_target_timeline"] != "2" {
			t.Fatalf("expected recovery_target_timeline to be 2, got %v", cfg["recovery_target_timeline"])
		}
	})
}

func TestPGUserConfigOverride(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		DataDir:                pgTestDirectory,
		Port:                   5433,
		ConfigFilePath:         pgConfigFilePath,
		InternalConfigFilePath: pgInternalConfigFilePath,
		UserConfigFilePath:     pgUserConfigFilePath,

		passwordFilePath: pgPasswordFilePath,
	}

	if err := stubPGConfigFile(); err != nil {
		t.Fatal(err)
	}

	store, _ := state.NewStore()
	if err := pgConf.initialize(store); err != nil {
		t.Error(err)
	}

	pgConf.SetUserConfig(ConfigMap{
		"log_statement": "ddl",
		"port":          "10000",
	})

	if err := WriteConfigFiles(pgConf); err != nil {
		t.Error(err)
	}

	cfg, err := pgConf.CurrentConfig()
	if err != nil {
		t.Fatal(err)
	}

	if cfg["port"] != "10000" {
		t.Fatalf("expected port to be 10000, got %v", cfg["port"])
	}

	if cfg["log_statement"] != "ddl" {
		t.Fatalf("expected log_statement to be ddl, got %v", cfg["log_statement"])
	}

	// Ensure defaults were not touched
	if cfg["max_connections"] != "300" {
		t.Fatalf("expected max_connections to be 300, got %v", cfg["max_connections"])
	}

	if cfg["hot_standby"] != "true" {
		t.Fatalf("expected hot_standby to be true, got %v", cfg["hot_standby"])
	}
}

func TestPGHBAConfig(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		DataDir:                pgTestDirectory,
		Port:                   5433,
		ConfigFilePath:         pgConfigFilePath,
		InternalConfigFilePath: pgInternalConfigFilePath,
		UserConfigFilePath:     pgUserConfigFilePath,

		passwordFilePath: pgPasswordFilePath,

		repmgrUsername: "repmgr",
		repmgrDatabase: "repgmr",
	}

	if err := pgConf.setDefaultHBA(); err != nil {
		t.Fatal(err)
	}

	if !utils.FileExists(pgHBAFilePath) {
		t.Fatalf("expected pg_hba.conf file to be present")
	}
}

func TestPGDefaultPassword(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		DataDir:                pgTestDirectory,
		Port:                   5433,
		ConfigFilePath:         pgConfigFilePath,
		InternalConfigFilePath: pgInternalConfigFilePath,
		UserConfigFilePath:     pgUserConfigFilePath,

		passwordFilePath: pgPasswordFilePath,
		repmgrUsername:   "repmgr",
		repmgrDatabase:   "repgmr",
	}

	targetPwd := "my-password"
	if err := pgConf.writePasswordFile(targetPwd); err != nil {
		t.Fatal(err)
	}

	if !utils.FileExists(pgConf.passwordFilePath) {
		t.Fatalf("expected pg_hba.conf file to be present")
	}

	pwdBytes, err := os.ReadFile(pgConf.passwordFilePath)
	if err != nil {
		t.Error(err)
	}

	if string(pwdBytes) != targetPwd {
		t.Fatalf("expected %s, got %s", targetPwd, string(pwdBytes))
	}
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

	if _, err := os.Stat(testBarmanConfigDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(testBarmanConfigDir, 0750); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func TestValidateCompatibility(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		DataDir:                pgTestDirectory,
		Port:                   5433,
		ConfigFilePath:         pgConfigFilePath,
		InternalConfigFilePath: pgInternalConfigFilePath,
		UserConfigFilePath:     pgUserConfigFilePath,

		passwordFilePath: pgPasswordFilePath,
		repmgrUsername:   "repmgr",
		repmgrDatabase:   "repgmr",
	}

	if err := stubPGConfigFile(); err != nil {
		t.Fatal(err)
	}

	store, _ := state.NewStore()
	if err := pgConf.initialize(store); err != nil {
		t.Fatal(err)
	}
	t.Run("SharedPreloadLibraries", func(t *testing.T) {
		valid := ConfigMap{
			"shared_preload_libraries": "repmgr",
		}
		conf, err := pgConf.validateCompatibility(valid)
		if err != nil {
			t.Fatal(err)
		}
		if conf["shared_preload_libraries"].(string) != "'repmgr'" {
			t.Fatal("expected preload library string to be wrapped in single quotes")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "'repmgr'",
		}
		conf, err = pgConf.validateCompatibility(valid)
		if err != nil {
			t.Fatal(err)
		}
		if conf["shared_preload_libraries"].(string) != "'repmgr'" {
			t.Fatal("expected preload library string to be wrapped in single quotes")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "repmgr,timescaledb",
		}
		conf, err = pgConf.validateCompatibility(valid)
		if err != nil {
			t.Fatal(err)
		}
		if conf["shared_preload_libraries"].(string) != "'repmgr,timescaledb'" {
			t.Fatal("expected preload library string to be wrapped in single quotes")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "",
		}
		if _, err := pgConf.validateCompatibility(valid); err == nil {
			t.Fatal("expected validation to fail when empty")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "timescaledb",
		}
		if _, err := pgConf.validateCompatibility(valid); err == nil {
			t.Fatal("expected validation to fail when repmgr is missing")
		}

	})

	t.Run("WalLevel", func(t *testing.T) {
		valid := ConfigMap{
			"wal_level": "replica",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		valid = ConfigMap{
			"wal_level": "logical",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		invalid := ConfigMap{
			"wal_level":       "logical",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}

		invalid = ConfigMap{
			"wal_level":       "replica",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}

	})

	t.Run("WalLevelMinimal", func(t *testing.T) {
		valid := ConfigMap{
			"wal_level":       "minimal",
			"archive_mode":    "off",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		invalid := ConfigMap{
			"wal_level":       "minimal",
			"archive_mode":    "on",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}

		invalid = ConfigMap{
			"wal_level":       "minimal",
			"archive_mode":    "off",
			"max_wal_senders": "10",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}

		invalid = ConfigMap{
			"wal_level": "minimal",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}
	})

	t.Run("maxWalSenders", func(t *testing.T) {
		valid := ConfigMap{
			"wal_level":       "minimal",
			"archive_mode":    "off",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		invalid := ConfigMap{
			"wal_level":       "replica",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}

		invalid = ConfigMap{
			"wal_level":       "logical",
			"max_wal_senders": "0",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}
	})

	t.Run("maxReplicationSlots", func(t *testing.T) {
		valid := ConfigMap{
			"wal_level":             "replica",
			"max_replication_slots": "10",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		valid = ConfigMap{
			"wal_level":             "logical",
			"max_replication_slots": "12",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		invalid := ConfigMap{
			"wal_level":             "minimal",
			"max_replication_slots": "20",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}
	})

	t.Run("maxConnections", func(t *testing.T) {
		valid := ConfigMap{
			"max_connections": "14",
		}
		if _, err := pgConf.validateCompatibility(valid); err != nil {
			t.Fatal(err)
		}

		invalid := ConfigMap{
			"max_connections": "4",
		}
		if _, err := pgConf.validateCompatibility(invalid); err == nil {
			t.Fatal(err)
		}
	})
}

func stubPGConfigFile() error {
	file, err := os.Create(pgConfigFilePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func cleanup() {
	if err := os.RemoveAll(pgTestDirectory); err != nil {
		fmt.Printf("failed to remove testing dir: %s\n", err)
	}
}
