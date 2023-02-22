package flypg

import (
	"testing"
)

const (
	repmgrTestDirectory          = "./test_results"
	repmgrConfigFilePath         = "./test_results/repmgr.conf"
	repgmrInternalConfigFilePath = "./test_results/repmgr.internal.conf"
	repgmrUserConfigFilePath     = "./test_results/repmgr.internal.conf"
)

func TestRepmgrConfigDefaults(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	conf := &RepMgr{
		AppName:            "test-app",
		PrimaryRegion:      "dev",
		Region:             "dev",
		ConfigPath:         repmgrConfigFilePath,
		InternalConfigPath: repgmrInternalConfigFilePath,
		UserConfigPath:     repgmrUserConfigFilePath,
		DataDir:            repmgrTestDirectory,
		PrivateIP:          "127.0.0.1",
		Port:               5433,
		DatabaseName:       "repmgr",
	}

	if err := conf.setDefaults(); err != nil {
		t.Error(err)
	}

	if conf.internalConfig["node_name"] != "'127.0.0.1'" {
		t.Fatalf("expected node_name to be '127.0.0.1', got %v", conf.internalConfig["node_name"])
	}

	if conf.internalConfig["node_id"] == "" {
		t.Fatalf("expected node_id to not be empty, got %q", conf.internalConfig["node_id"])
	}

}

func TestRepmgrNodeIDGeneration(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	conf := &RepMgr{
		AppName:            "test-app",
		PrimaryRegion:      "dev",
		Region:             "dev",
		ConfigPath:         repmgrConfigFilePath,
		InternalConfigPath: repgmrInternalConfigFilePath,
		UserConfigPath:     repgmrUserConfigFilePath,
		DataDir:            repmgrTestDirectory,
		PrivateIP:          "127.0.0.1",
		Port:               5433,
		DatabaseName:       "repmgr",
	}

	if err := conf.setDefaults(); err != nil {
		t.Error(err)
	}

	if err := writeInternalConfigFile(conf); err != nil {
		t.Error(err)
	}

	nodeID := conf.internalConfig["node_id"]

	resolvedNodeID, err := conf.resolveNodeID()
	if err != nil {
		t.Error(err)
	}

	if nodeID != resolvedNodeID {
		t.Fatalf("expected node_id to be %s, got %q", nodeID, resolvedNodeID)
	}
}
