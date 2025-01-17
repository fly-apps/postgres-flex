package flypg

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	repmgrTestDirectory          = "./test_results"
	repmgrConfigFilePath         = "./test_results/repmgr.conf"
	repgmrInternalConfigFilePath = "./test_results/repmgr.internal.conf"
	repgmrUserConfigFilePath     = "./test_results/repmgr.user.conf"
	repgmrPasswordConfigFilePath = "./test_results/.pgpass"
)

func TestRepmgrInitialization(t *testing.T) {
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
		PasswordConfigPath: repgmrPasswordConfigFilePath,
		DataDir:            repmgrTestDirectory,
		MachineID:          "abcdefg1234567",
		PrivateIP:          "127.0.0.1",
		Credentials: admin.Credential{
			Username: "user",
			Password: "password",
		},
	}

	t.Run("initializate", func(t *testing.T) {
		if err := conf.initialize(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("repmgr.conf", func(t *testing.T) {
		contents, err := os.ReadFile(conf.ConfigPath)
		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(string(contents), "repmgr.internal.conf") {
			t.Fatalf("expected %s to include repmgr.internal.conf", conf.ConfigPath)
		}

		if !strings.Contains(string(contents), "repmgr.user.conf") {
			t.Fatalf("expected %s to include repmgr.user.conf", conf.ConfigPath)
		}
	})

	t.Run(".pgpass", func(t *testing.T) {
		pwd, err := os.ReadFile(conf.PasswordConfigPath)
		if err != nil {
			t.Error(err)
		}

		expectedPwd := fmt.Sprintf("*:*:*:%s:%s", conf.Credentials.Username, conf.Credentials.Password)
		if string(pwd) != expectedPwd {
			t.Fatalf("expected %s to contain %s, but got %s", conf.PasswordConfigPath, expectedPwd, string(pwd))
		}
	})

	t.Run("repmgr.internal.conf", func(t *testing.T) {
		if !utils.FileExists(conf.InternalConfigPath) {
			t.Fatalf("expected %s to exist, but doesn't", conf.InternalConfigPath)
		}
	})

	t.Run("repmgr.internal.conf", func(t *testing.T) {
		if !utils.FileExists(conf.UserConfigPath) {
			t.Fatalf("expected %s to exist, but doesn't", conf.UserConfigPath)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		config, err := conf.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if config["node_name"] != "'abcdefg1234567'" {
			t.Fatalf("expected node_name to be 'abcdefg1234567', got %v", config["node_name"])
		}

		if config["location"] != "'dev'" {
			t.Fatalf("expected location to eq 'dev', but got %q", config["location"])
		}

		if config["node_id"] == "" {
			t.Fatalf("expected node_id to not be empty, got %q", config["node_id"])
		}
	})
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
		PasswordConfigPath: repgmrPasswordConfigFilePath,

		DataDir:      repmgrTestDirectory,
		PrivateIP:    "127.0.0.1",
		MachineID:    "abcdefg1234567",
		Port:         5433,
		DatabaseName: "repmgr",
		Credentials: admin.Credential{
			Username: "user",
			Password: "password",
		},
	}

	if err := conf.initialize(); err != nil {
		t.Fatal(err)
	}

	config, err := conf.CurrentConfig()
	if err != nil {
		t.Fatal(err)
	}

	nodeID := config["node_id"]

	resolvedNodeID, err := conf.resolveNodeID()
	if err != nil {
		t.Fatal(err)
	}

	if nodeID != resolvedNodeID {
		t.Fatalf("expected node_id to be %s, got %q", nodeID, resolvedNodeID)
	}
}
