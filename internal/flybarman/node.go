package flybarman

import (
	"context"
	"fmt"
	"io/ioutil"
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
		PasswordConfigPath: dataDir + "/.pgpass",
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

func (n *Node) Init(ctx context.Context) error {
	err := flypg.WriteSSHKey()
	if err != nil {
		return fmt.Errorf("failed write ssh keys: %s", err)
	}

	if _, err := os.Stat(barmanConfigFile); os.IsNotExist(err) {
		barmanConfigFileContent := fmt.Sprintf(`[barman]
barman_user = root
barman_home = /data/barman-home
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

		if err := ioutil.WriteFile(barmanConfigFile, []byte(barmanConfigFileContent), 0644); err != nil {
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

	log.Println("Symbolic link created successfully.")

	if err := os.MkdirAll(n.BarmanHome, os.ModePerm); err != nil {
		return fmt.Errorf("failed to mkdir %s: %s", n.BarmanHome, err)
	}

	log.Println("Barman home directory successfully.")

	passStr := fmt.Sprintf("*:*:*:%s:%s", n.ReplCredentials.Username, n.ReplCredentials.Password)
	if err := os.WriteFile(n.PasswordConfigPath, []byte(passStr), 0700); err != nil {
		return fmt.Errorf("failed to write file %s: %s", n.PasswordConfigPath, err)
	}

	if _, err := os.Stat(barmanCronFile); os.IsNotExist(err) {
		barmanCronFileContent := `* * * * * /usr/bin/barman cron
`
		if err := ioutil.WriteFile(barmanCronFile, []byte(barmanCronFileContent), 0644); err != nil {
			return fmt.Errorf("failed write %s: %s", barmanCronFile, err)
		}

		log.Println(barmanCronFile + " created successfully.")
	}

	if _, err := os.Stat(n.LogFile); os.IsNotExist(err) {
		file, err := os.Create(n.LogFile)
		if err != nil {
			return fmt.Errorf("failed to touch %s: %s", n.LogFile, err)
		}
		defer file.Close()

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
