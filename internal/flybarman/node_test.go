package flybarman

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
)

var (
	barmanTestDirectory              = "./test_results"
	barmanTestBarmanConfigFile       = barmanTestDirectory + "/barman.conf"
	barmanTestBarmanCronFile         = barmanTestDirectory + "/barman.cron"
	barmanTestGlobalBarmanConfigFile = barmanTestDirectory + "/global-barman-file.conf"
	barmanTestBarmanHome             = barmanTestDirectory + "/barman.d"
	barmanTestLogFile                = barmanTestDirectory + "/barman.log"
	barmanTestPasswordConfigPath     = barmanTestDirectory + "/.pgpass"
)

func TestBarmanInitialization(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	node := &Node{
		AppName:                "local",
		BarmanConfigFile:       barmanTestBarmanConfigFile,
		BarmanCronFile:         barmanTestBarmanCronFile,
		GlobalBarmanConfigFile: barmanTestGlobalBarmanConfigFile,
		BarmanHome:             barmanTestBarmanHome,
		LogFile:                barmanTestLogFile,
		PasswordConfigPath:     barmanTestPasswordConfigPath,
		ReplCredentials: admin.Credential{
			Username: "user",
			Password: "password",
		},
	}

	ctx := context.Background()

	t.Run("initializate", func(t *testing.T) {
		if err := node.Init(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("barman.conf", func(t *testing.T) {
		contents, err := os.ReadFile(node.BarmanConfigFile)
		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(string(contents), "Fly.io Postgres Cluster") {
			t.Fatalf("expected %s to include Fly.io Postgres Cluster", node.BarmanConfigFile)
		}

		if !strings.Contains(string(contents), "barman_user = root") {
			t.Fatalf("expected %s to include barman_user = root", node.BarmanConfigFile)
		}
	})

	t.Run("global barman.conf symlink", func(t *testing.T) {
		fileInfo, err := os.Lstat(node.GlobalBarmanConfigFile)
		if err != nil {
			fmt.Println("failed Lstat for global barman symlink:", err)
			return
		}

		if fileInfo.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("File is not a symbolic link")
		}
	})

	t.Run(".pgpass", func(t *testing.T) {
		pwd, err := os.ReadFile(node.PasswordConfigPath)
		if err != nil {
			t.Error(err)
		}

		expectedPwd := fmt.Sprintf("*:*:*:%s:%s", node.ReplCredentials.Username, node.ReplCredentials.Password)
		if string(pwd) != expectedPwd {
			t.Fatalf("expected %s to contain %s, but got %s", node.PasswordConfigPath, expectedPwd, string(pwd))
		}
	})
}

func setup(t *testing.T) error {
	t.Setenv("FLY_VM_MEMORY_MB", fmt.Sprint(256*(1024*1024)))
	t.Setenv("UNIT_TESTING", "true")

	if _, err := os.Stat(barmanTestDirectory); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(barmanTestDirectory, 0750); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func cleanup() {
	if err := os.RemoveAll(barmanTestDirectory); err != nil {
		fmt.Printf("failed to remove testing dir: %s\n", err)
	}
}
