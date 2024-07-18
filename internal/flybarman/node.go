package flybarman

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
)

var (
	dataDir                = "/data"
	barmanConfigFile       = dataDir + "/barman.conf"
	barmanCronFile         = dataDir + "/barman.cron"
	globalBarmanConfigFile = "/etc/barman.conf"
	barmanHome             = dataDir + "/barman.d"
	logFile                = dataDir + "/barman.log"
	passwordConfigPath     = "/data/.pgpass"
	rootPasswordConfigPath = "/.pgpass"
)

type Node struct {
	AppName       string
	PrivateIP     string
	PrimaryRegion string
	DataDir       string
	Port          int

	BarmanConfigFile       string
	BarmanCronFile         string
	GlobalBarmanConfigFile string
	BarmanHome             string
	LogFile                string
	PasswordConfigPath     string
	RootPasswordConfigPath string

	SUCredentials       admin.Credential
	OperatorCredentials admin.Credential
	ReplCredentials     admin.Credential
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName:                "local",
		BarmanConfigFile:       barmanConfigFile,
		BarmanCronFile:         barmanCronFile,
		GlobalBarmanConfigFile: globalBarmanConfigFile,
		BarmanHome:             barmanHome,
		LogFile:                logFile,
		PasswordConfigPath:     passwordConfigPath,
		RootPasswordConfigPath: rootPasswordConfigPath,
	}

	if appName := os.Getenv("FLY_APP_NAME"); appName != "" {
		node.AppName = appName
	}

	// Internal user
	node.SUCredentials = admin.Credential{
		Username: "flypgadmin",
		Password: os.Getenv("SU_PASSWORD"),
	}

	// Postgres user
	node.OperatorCredentials = admin.Credential{
		Username: "postgres",
		Password: os.Getenv("OPERATOR_PASSWORD"),
	}

	// Repmgr user
	node.ReplCredentials = admin.Credential{
		Username: "repmgr",
		Password: os.Getenv("REPL_PASSWORD"),
	}

	return node, nil
}

func (n *Node) Init(_ context.Context) error {
	if os.Getenv("UNIT_TESTING") == "" {
		err := flypg.WriteSSHKey()
		if err != nil {
			return fmt.Errorf("failed write ssh keys: %s", err)
		}
	}

	if _, err := os.Stat(n.BarmanConfigFile); os.IsNotExist(err) {
		barmanConfigFileContent := fmt.Sprintf(`[barman]
barman_user = root
barman_home = /data/barman.d
log_level = info
log_file = /data/barman.log

[pg]
description =  "Fly.io Postgres Cluster"
conninfo = host=%s.internal user=repmgr dbname=postgres
streaming_conninfo = host=%s.internal user=repmgr dbname=postgres
backup_method = postgres
streaming_archiver = on
slot_name = barman
create_slot = auto
retention_policy_mode = auto
retention_policy = RECOVERY WINDOW OF 7 days
wal_retention_policy = main
`, n.AppName, n.AppName)

		if err := os.WriteFile(n.BarmanConfigFile, []byte(barmanConfigFileContent), 0644); err != nil {
			return fmt.Errorf("failed write %s: %s", n.BarmanConfigFile, err)
		}

		log.Println(n.BarmanConfigFile + " created successfully.")
	}

	if err := n.deleteGlobalBarmanFile(); err != nil {
		return fmt.Errorf("failed delete /etc/barman.conf: %s", err)
	}

	if err := os.Symlink(n.BarmanConfigFile, n.GlobalBarmanConfigFile); err != nil {
		return fmt.Errorf("failed symlink %s to %s: %s", n.BarmanConfigFile, n.GlobalBarmanConfigFile, err)
	}

	log.Println("Symbolic link to barman config created successfully.")

	if err := os.MkdirAll(n.BarmanHome, os.ModePerm); err != nil {
		return fmt.Errorf("failed to mkdir %s: %s", n.BarmanHome, err)
	}

	log.Println("Barman home directory successfully.")

	passStr := fmt.Sprintf("*:*:*:%s:%s", n.ReplCredentials.Username, n.ReplCredentials.Password)
	if err := os.WriteFile(n.PasswordConfigPath, []byte(passStr), 0700); err != nil {
		return fmt.Errorf("failed to write file %s: %s", n.PasswordConfigPath, err)
	}
	// We need this in case the user ssh to the vm as root
	if err := os.WriteFile(n.RootPasswordConfigPath, []byte(passStr), 0700); err != nil {
		return fmt.Errorf("failed to write file %s: %s", n.RootPasswordConfigPath, err)
	}

	barmanCronFileContent := `* * * * * /usr/local/bin/barman_cron
	`

	if err := os.WriteFile(n.BarmanCronFile, []byte(barmanCronFileContent), 0644); err != nil {
		return fmt.Errorf("failed write %s: %s", n.BarmanCronFile, err)
	}
	log.Println(n.BarmanCronFile + " created successfully.")

	if _, err := os.Stat(n.LogFile); os.IsNotExist(err) {
		file, err := os.Create(n.LogFile)
		if err != nil {
			return fmt.Errorf("failed to touch %s: %s", n.LogFile, err)
		}
		defer func() { _ = file.Close() }()

		log.Println(n.LogFile + " created successfully.")
	}

	if os.Getenv("UNIT_TESTING") == "" {
		crontabCommand := exec.Command("/usr/bin/crontab", n.BarmanCronFile)
		if _, err := crontabCommand.Output(); err != nil {
			return fmt.Errorf("failed set crontab: %s", err)
		}

		log.Println("Crontab updated")

		switchWalCommand := exec.Command("barman", "switch-wal", "--archive", "--force", "pg")
		if _, err := switchWalCommand.Output(); err != nil {
			log.Println(fmt.Errorf("failed switching WAL: %s", err))
			log.Println("try running `barman switch-wal --archive --force pg` or wait for the next WAL")
		} else {
			log.Println("successfully switched WAL files to start barman")
		}

		cronCommand := exec.Command("barman", "cron")
		if _, err := cronCommand.Output(); err != nil {
			log.Println(fmt.Errorf("failed running barman cron: %s", err))
			log.Println("try running `cronCommand` or wait for the next run")
		} else {
			log.Println("successfully ran `barman cron`")
		}
	}

	return nil
}

func (n *Node) deleteGlobalBarmanFile() error {
	if _, err := os.Stat(n.GlobalBarmanConfigFile); os.IsNotExist(err) {
		return nil
	}

	if err := os.Remove(n.GlobalBarmanConfigFile); err != nil {
		return err
	}

	log.Println(n.GlobalBarmanConfigFile + " deleted successfully")
	return nil
}
