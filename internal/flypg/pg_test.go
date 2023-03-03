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

	pgHBAFilePath = "./test_results/pg_hba.conf"
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

	return nil
}

func TestValidateConfiguration(t *testing.T) {
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
	pgConf.initialize(store)

	t.Run("validSharedPreloadLibraries", func(t *testing.T) {
		valid := ConfigMap{
			"shared_preload_libraries": "repmgr",
		}

		conf, err := pgConf.Validate(valid)
		if err != nil {
			t.Fatal(err)
		}

		if conf["shared_preload_libraries"].(string) != "'repmgr'" {
			t.Fatal("expected preload library string to be wrapped in single quotes")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "'repmgr'",
		}

		conf, err = pgConf.Validate(valid)
		if err != nil {
			t.Fatal(err)
		}

		if conf["shared_preload_libraries"].(string) != "'repmgr'" {
			t.Fatal("expected preload library string to be wrapped in single quotes")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "repmgr,timescaledb",
		}

		conf, err = pgConf.Validate(valid)
		if err != nil {
			t.Fatal(err)
		}

		pgConf.SetUserConfig(conf)

		fmt.Printf("%+v\n", pgConf.userConfig)

		if conf["shared_preload_libraries"].(string) != "'repmgr,timescaledb'" {
			t.Fatal("expected preload library string to be wrapped in single quotes")
		}

	})

	t.Run("invalidSharedPreloadLibraries", func(t *testing.T) {
		valid := ConfigMap{
			"shared_preload_libraries": "",
		}

		if _, err := pgConf.Validate(valid); err == nil {
			t.Fatal("expected validation to fail when empty")
		}

		valid = ConfigMap{
			"shared_preload_libraries": "timescaledb",
		}

		if _, err := pgConf.Validate(valid); err == nil {
			t.Fatal("expected validation to fail when repmgr is missing")
		}
	})

	t.Run("validWalLevel", func(t *testing.T) {
		valid := ConfigMap{
			"wal_level": "replica",
		}

		if _, err := pgConf.Validate(valid); err != nil {
			t.Fatal(err)
		}

		valid = ConfigMap{
			"wal_level": "logical",
		}

		if _, err := pgConf.Validate(valid); err != nil {
			t.Fatal(err)
		}

		valid = ConfigMap{
			"wal_level":    "minimal",
			"archive_mode": "off",
		}

		if _, err := pgConf.Validate(valid); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("invalidWalLevel", func(t *testing.T) {
		valid := ConfigMap{
			"wal_level": "minimal",
		}

		if _, err := pgConf.Validate(valid); err == nil {
			t.Fatal("expected wal_level minimal to fail with archiving enabled")
		}

		valid = ConfigMap{
			"wal_level": "logical",
		}

		if _, err := pgConf.Validate(valid); err != nil {
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
