package flypg

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-standalone/pkg/flypg/admin"
	"github.com/fly-apps/postgres-standalone/pkg/flypg/state"
	"github.com/fly-apps/postgres-standalone/pkg/privnet"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type Credentials struct {
	Username string
	Password string
}

type Node struct {
	AppName   string
	PrivateIP net.IP
	DataDir   string
	PGPort    int

	SUCredentials       Credentials
	OperatorCredentials Credentials
	ManagerCredentials  Credentials

	ManagerConfigPath   string
	ManagerDatabaseName string
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName:             "local",
		PGPort:              5432,
		DataDir:             "/data/postgresql",
		ManagerDatabaseName: "repmgr",
		ManagerConfigPath:   "/data/repmgr.conf",
	}

	if appName := os.Getenv("FLY_APP_NAME"); appName != "" {
		node.AppName = appName
	}

	ipv6, err := privnet.PrivateIPv6()
	if err != nil {
		return nil, fmt.Errorf("failed getting private ip: %s", err)
	}

	node.PrivateIP = ipv6

	if port, err := strconv.Atoi(os.Getenv("PG_PORT")); err == nil {
		node.PGPort = port
	}

	node.SUCredentials = Credentials{
		Username: "flypgadmin",
		Password: os.Getenv("SU_PASSWORD"),
	}

	node.OperatorCredentials = Credentials{
		Username: "postgres",
		Password: os.Getenv("OPERATOR_PASSWORD"),
	}

	node.ManagerCredentials = Credentials{
		Username: "repmgr",
		Password: "supersecret",
	}

	return node, nil
}

func (n *Node) Init() error {
	if err := setDirOwnership(); err != nil {
		return err
	}

	client, err := state.NewConsulClient()
	if err != nil {
		return fmt.Errorf("failed to establish connection with consul: %s", err)
	}

	// Check to see if there's already a registered primary.
	leaderIP, err := client.CurrentPrimary()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	if leaderIP == n.PrivateIP.String() {
		return nil
	}

	if err := InitializeManager(*n); err != nil {
		fmt.Printf("Failed to initialize replmgr: %s\n", err.Error())
	}

	if leaderIP == "" {
		fmt.Println("Initializing postgres")
		if err := n.initializePostgres(); err != nil {
			return fmt.Errorf("failed to initialize postgres %s", err)
		}

		fmt.Println("Setting default HBA")
		if err := n.setDefaultHBA(); err != nil {
			return fmt.Errorf("failed updating pg_hba.conf: %s", err)
		}
	} else {
		fmt.Println("Cloning from primary")
		if err := cloneFromPrimary(*n, leaderIP); err != nil {
			return fmt.Errorf("failed to clone primary: %s", err)
		}
	}

	fmt.Println("Configuring postgres")
	if err := n.configurePostgres(); err != nil {
		return fmt.Errorf("failed to configure postgres %s", err)
	}

	return nil
}

func (n *Node) PostInit() error {
	client, err := state.NewConsulClient()
	if err != nil {
		return fmt.Errorf("failed to establish connection with consul: %s", err)
	}

	// Check to see if there's already a registered primary.
	leaderIP, err := client.CurrentPrimary()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	switch leaderIP {
	case "":
		conn, err := n.NewLocalConnection(context.TODO())
		if err != nil {
			return err
		}

		if err := n.createRequiredUsers(conn); err != nil {
			return fmt.Errorf("failed to create required users: %s", err)
		}

		fmt.Println("Creating metadata db")
		if _, err := admin.CreateDatabase(conn, n.ManagerDatabaseName, n.ManagerCredentials.Username); err != nil {
			return err
		}

		fmt.Println("Enabling extensions")
		if err := admin.EnableExtension(conn, "repmgr"); err != nil {
			return err
		}

		fmt.Println("Registering Primary")

		if err := registerPrimary(*n); err != nil {
			fmt.Printf("failed to register primary: %s", err)
		}

		if err := client.RegisterPrimary(n.PrivateIP.String()); err != nil {
			return fmt.Errorf("failed to register primary with consul: %s", err)
		}
	case n.PrivateIP.String():
		if err := registerPrimary(*n); err != nil {
			fmt.Printf("failed to register primary: %s", err)
		}

		fmt.Println("Nothing to do here")
	default:
		// TODO - We need to track registered standbys to we don't re-register outselves.
		fmt.Println("Registering standby")
		if err := registerStandby(*n); err != nil {
			fmt.Printf("failed to register standby: %s\n", err)
		}
	}

	return nil
}

func (n *Node) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP.String(), strconv.Itoa(n.PGPort))
	return openConnection(ctx, host, n.OperatorCredentials)
}

func (n *Node) createRequiredUsers(conn *pgx.Conn) error {
	curUsers, err := admin.ListUsers(context.TODO(), conn)
	if err != nil {
		return errors.Wrap(err, "failed to list current users")
	}

	credMap := map[string]string{
		n.SUCredentials.Username:       n.SUCredentials.Password,
		n.OperatorCredentials.Username: n.OperatorCredentials.Password,
		n.ManagerCredentials.Username:  n.ManagerCredentials.Password,
	}

	for user, pass := range credMap {
		exists := false
		for _, curUser := range curUsers {
			if user == curUser.Username {
				exists = true
			}
		}
		var sql string

		if exists {
			sql = fmt.Sprintf("ALTER USER %s WITH PASSWORD '%s'", user, pass)
		} else {
			fmt.Printf("Creating %s\n", user)
			sql = fmt.Sprintf(`CREATE USER %s WITH SUPERUSER LOGIN PASSWORD '%s'`, user, pass)
			_, err := conn.Exec(context.Background(), sql)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Node) initializePostgres() error {
	_, err := os.Stat(n.DataDir)
	if os.IsNotExist(err) {
		if err := ioutil.WriteFile("/data/.default_password", []byte(os.Getenv("OPERATOR_PASSWORD")), 0644); err != nil {
			return err
		}

		cmd := exec.Command("gosu", "postgres", "initdb", "--pgdata", n.DataDir, "--pwfile=/data/.default_password")
		_, err := cmd.CombinedOutput()
		if err != nil {
			return err
		}

		return nil
	}

	return err
}

func (n *Node) configurePostgres() error {
	cmdStr := fmt.Sprintf("sed -i \"s/#shared_preload_libraries.*/shared_preload_libraries = 'repmgr'/\" /data/postgresql/postgresql.conf")

	return runCommand(cmdStr)
}

type HBAEntry struct {
	Type     string
	Database string
	User     string
	Address  string
	Method   string
}

func (n *Node) setDefaultHBA() error {
	var entries []HBAEntry

	entries = []HBAEntry{
		{
			Type:     "local",
			Database: "all",
			User:     "postgres",
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: n.ManagerDatabaseName,
			User:     n.ManagerCredentials.Username,
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: "replication",
			User:     n.ManagerCredentials.Username,
			Method:   "trust",
		},
		{
			Type:     "host",
			Database: "replication",
			User:     n.ManagerCredentials.Username,
			Address:  "::0/0",
			Method:   "trust",
		},
		{
			Type:     "host",
			Database: n.ManagerDatabaseName,
			User:     n.ManagerCredentials.Username,
			Address:  "::0/0",
			Method:   "trust",
		},
		{
			Type:     "host",
			Database: "all",
			User:     "all",
			Address:  "0.0.0.0/0",
			Method:   "md5",
		},
		{
			Type:     "host",
			Database: "all",
			User:     "all",
			Address:  "::0/0",
			Method:   "md5",
		},
	}

	path := fmt.Sprintf("%s/pg_hba.conf", n.DataDir)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		str := fmt.Sprintf("%s %s %s %s %s\n", entry.Type, entry.Database, entry.User, entry.Address, entry.Method)
		_, err := file.Write([]byte(str))
		if err != nil {
			return err
		}
	}

	return nil
}

func openConnection(ctx context.Context, host string, creds Credentials) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("postgres://%s/postgres", host)
	conf, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	conf.User = creds.Username
	conf.Password = creds.Password
	conf.ConnectTimeout = 5 * time.Second

	conn, err := pgx.ConnectConfig(ctx, conf)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func setDirOwnership() error {
	pgUser, err := user.Lookup("postgres")
	if err != nil {
		return err
	}
	pgUID, err := strconv.Atoi(pgUser.Uid)
	if err != nil {
		return err
	}
	pgGID, err := strconv.Atoi(pgUser.Gid)
	if err != nil {
		return err
	}

	cmdStr := fmt.Sprintf("chown -R %d:%d %s", pgUID, pgGID, "/data")
	cmd := exec.Command("sh", "-c", cmdStr)
	_, err = cmd.Output()
	if err != nil {
		return err
	}

	return nil
}

func runCommand(cmdStr string) error {
	pgUser, err := user.Lookup("postgres")
	if err != nil {
		return err
	}
	pgUID, err := strconv.Atoi(pgUser.Uid)
	if err != nil {
		return err
	}
	pgGID, err := strconv.Atoi(pgUser.Gid)
	if err != nil {
		return err
	}

	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(pgUID), Gid: uint32(pgGID)}
	_, err = cmd.Output()
	if err != nil {
		return err
	}

	return nil
}
