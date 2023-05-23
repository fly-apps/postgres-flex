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

var dataDir = "/data"
var barmanConfigFile = dataDir + "/barman.conf"
var barmanCronFile = dataDir + "/barman.cron"
var globalBarmanConfigFile = "/etc/barman.conf"

type Node struct {
	AppName       string
	PrivateIP     string
	PrimaryRegion string
	DataDir       string
	Port          int

	BarmanHome         string
	LogFile            string
	PasswordConfigPath string

	SUCredentials       admin.Credential
	OperatorCredentials admin.Credential
	ReplCredentials     admin.Credential
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName:            "local",
		BarmanHome:         dataDir + "/barman.d",
		LogFile:            dataDir + "/barman.log",
		PasswordConfigPath: "/root/.pgpass",
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
	err := flypg.WriteSSHKey()
	if err != nil {
		return fmt.Errorf("failed write ssh keys: %s", err)
	}

	if _, err := os.Stat(barmanConfigFile); os.IsNotExist(err) {
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

		if err := os.WriteFile(barmanConfigFile, []byte(barmanConfigFileContent), 0644); err != nil {
			return fmt.Errorf("failed write %s: %s", barmanConfigFile, err)
		}

		log.Println(barmanConfigFile + " created successfully.")
	}

	if err := deleteGlobalBarmanFile(); err != nil {
		return fmt.Errorf("failed delete /etc/barman.conf: %s", err)
	}

	if err := os.Symlink(barmanConfigFile, globalBarmanConfigFile); err != nil {
		return fmt.Errorf("failed symlink %s to %s: %s", barmanConfigFile, globalBarmanConfigFile, err)
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
	if err := os.WriteFile("/.pgpass", []byte(passStr), 0700); err != nil {
		return fmt.Errorf("failed to write file %s: %s", n.PasswordConfigPath, err)
	}

	if _, err := os.Stat(barmanCronFile); os.IsNotExist(err) {
		barmanCronFileContent := `* * * * * /usr/bin/barman cron
`
		if err := os.WriteFile(barmanCronFile, []byte(barmanCronFileContent), 0644); err != nil {
			return fmt.Errorf("failed write %s: %s", barmanCronFile, err)
		}

		log.Println(barmanCronFile + " created successfully.")
	}

	if _, err := os.Stat(n.LogFile); os.IsNotExist(err) {
		file, err := os.Create(n.LogFile)
		if err != nil {
			return fmt.Errorf("failed to touch %s: %s", n.LogFile, err)
		}
		defer func() { _ = file.Close() }()

		log.Println(n.LogFile + " created successfully.")
	}

	crontabCommand := exec.Command("/usr/bin/crontab", barmanCronFile)
	if _, err := crontabCommand.Output(); err != nil {
		return fmt.Errorf("failed set crontab: %s", err)
	}

	log.Println("Crontab updated")

	serviceCmd := exec.Command("/usr/sbin/service", "--version")
	if err := serviceCmd.Run(); err != nil {
		log.Println("service command not found, skipping initializing cron service")
	} else {
		serviceCronStartCommand := exec.Command("service", "cron", "start")
		if _, err := serviceCronStartCommand.Output(); err != nil {
			return fmt.Errorf("failed starting cron service: %s", err)
		}
		log.Println("Started cron service")
	}

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

	return nil
}

func deleteGlobalBarmanFile() error {
	if _, err := os.Stat(globalBarmanConfigFile); os.IsNotExist(err) {
		return nil
	}

	if err := os.Remove(globalBarmanConfigFile); err != nil {
		return err
	}

	log.Println(globalBarmanConfigFile + " deleted successfully")
	return nil
}
