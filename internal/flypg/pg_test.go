package flypg

import (
	"fmt"
	"os"
	"testing"
)

const (
	pgTestDirectory          = "./test_results"
	pgConfigFilePath         = "./test_results/postgresql.conf"
	pgInternalConfigFilePath = "./test_results/postgresql.internal.conf"
	pgUserConfigFilePath     = "./test_results/postgresql.user.conf"
)

func TestPGConfigDefaults(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		dataDir:                pgTestDirectory,
		port:                   5433,
		configFilePath:         pgConfigFilePath,
		internalConfigFilePath: pgInternalConfigFilePath,
		userConfigFilePath:     pgUserConfigFilePath,
		userConfig:             ConfigMap{},
		internalConfig:         ConfigMap{},
	}

	if err := pgConf.initialize(); err != nil {
		t.Error(err)
	}

	cfg, err := ReadFromFile(pgInternalConfigFilePath)
	if err != nil {
		t.Error(err)
	}

	if cfg["port"] != "5433" {
		t.Fatalf("expected port to be 5433, got %v", cfg["port"])
	}

	if cfg["hot_standby"] != "true" {
		t.Fatalf("expected hot_standby to be true, got %v", cfg["hot_standby"])
	}
}

func TestPGSettingOverride(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	pgConf := &PGConfig{
		dataDir:                pgTestDirectory,
		port:                   5433,
		configFilePath:         pgConfigFilePath,
		internalConfigFilePath: pgInternalConfigFilePath,
		userConfigFilePath:     pgUserConfigFilePath,
	}

	if err := pgConf.initialize(); err != nil {
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

	file, err := os.Create(pgConfigFilePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return file.Sync()

}

func cleanup() {
	os.RemoveAll(pgTestDirectory)
}
