package flypg

import (
	"testing"
	"time"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	flyTestDirectory          = "./test_results"
	flyInternalConfigFilePath = "./test_results/flypg.internal.conf"
	flyUserConfigFilePath     = "./test_results/flypg.user.conf"
)

func TestFlyConfigInitialization(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	cfg := FlyPGConfig{
		internalConfigFilePath: flyInternalConfigFilePath,
		userConfigFilePath:     flyInternalConfigFilePath,
	}

	if err := cfg.initialize(); err != nil {
		t.Fatal(err)
	}

	t.Run("configFiles", func(t *testing.T) {
		if !utils.FileExists(cfg.internalConfigFilePath) {
			t.Fatalf("expected %s to exist, but doesn't", cfg.internalConfigFilePath)
		}

		if !utils.FileExists(cfg.userConfigFilePath) {
			t.Fatalf("expected %s to exist, but doesn't", cfg.userConfigFilePath)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		cfg, err := cfg.CurrentConfig()
		if err != nil {
			t.Fatal(err)
		}

		if cfg["deadMemberRemovalThreshold"] != (24 * time.Hour).String() {
			t.Fatalf("expected deadMemberRemovalThreshold to be 24h, but got %v", cfg["deadMemberRemovalThreshold"])
		}
	})
}
