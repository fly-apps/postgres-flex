package flypg

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
	"github.com/fly-apps/postgres-flex/pkg/privnet"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type Credentials struct {
	Username string
	Password string
}

type Node struct {
	ID        int32
	AppName   string
	PrivateIP string
	DataDir   string
	Region    string
	PGPort    int
	ProxyPort int

	SUCredentials       Credentials
	OperatorCredentials Credentials
	ManagerCredentials  Credentials

	ManagerConfigPath   string
	ManagerDatabaseName string
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName:             "local",
		PGPort:              5433,
		ProxyPort:           5432,
		DataDir:             "/data/postgresql",
		ManagerDatabaseName: "repmgr",
		ManagerConfigPath:   "/data/repmgr.conf",
		Region:              os.Getenv("FLY_REGION"),
	}

	if appName := os.Getenv("FLY_APP_NAME"); appName != "" {
		node.AppName = appName
	}

	ipv6, err := privnet.PrivateIPv6()
	if err != nil {
		return nil, fmt.Errorf("failed getting private ip: %s", err)
	}
	node.PrivateIP = ipv6.String()

	machineID := os.Getenv("FLY_ALLOC_ID")
	// Generate a random, reconstructable signed int32
	seed := binary.LittleEndian.Uint64([]byte(machineID))
	rand.Seed(int64(seed))
	node.ID = rand.Int31()

	if port, err := strconv.Atoi(os.Getenv("PG_PORT")); err == nil {
		node.PGPort = port
	}

	// Internal user
	node.SUCredentials = Credentials{
		Username: "flypgadmin",
		Password: os.Getenv("SU_PASSWORD"),
	}

	// Superuser
	node.OperatorCredentials = Credentials{
		Username: "postgres",
		Password: os.Getenv("OPERATOR_PASSWORD"),
	}

	// Replication manager user
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
	primaryIP, err := client.CurrentPrimary()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	// Writes or updates the replication manager configuration.
	if err := configureRepmgr(*n); err != nil {
		fmt.Printf("Failed to initialize replmgr: %s\n", err.Error())
	}

	// We are done here if we are the primary.
	if primaryIP == n.PrivateIP {
		return nil
	}

	// If there's no primary then we should initialize ourself as the primary.
	if primaryIP == "" {
		fmt.Println("Initializing postgres")
		if err := n.initializePostgres(); err != nil {
			return fmt.Errorf("failed to initialize postgres %s", err)
		}

		fmt.Println("Setting default HBA")
		if err := n.setDefaultHBA(); err != nil {
			return fmt.Errorf("failed updating pg_hba.conf: %s", err)
		}
	} else {
		clonePrimary := true
		// Check to see if we've already been initialized.
		if n.isInitialized() {
			remoteConn, err := n.NewRepRemoteConnection(context.TODO(), primaryIP)
			if err != nil {
				return fmt.Errorf("failed to resolve my role according to the primary: %s", err)
			}
			role, err := memberRoleByHostname(context.TODO(), remoteConn, n.PrivateIP)
			// Don't re-clone if we are already a standby.
			if role == standbyRoleName {
				clonePrimary = false
			}
		}

		if clonePrimary {
			fmt.Println("Cloning from primary")
			if err := cloneFromPrimary(*n, primaryIP); err != nil {
				return fmt.Errorf("failed to clone primary: %s", err)
			}
		}
	}

	fmt.Println("Configuring postgres")
	if err := n.configurePostgres(); err != nil {
		return fmt.Errorf("failed to configure postgres %s", err)
	}

	fmt.Println("Configuring pgbouncer auth")
	if err := n.ConfigurePGBouncerAuth(); err != nil {
		return fmt.Errorf("failed to configure pgbouncer auth %s", err)
	}

	return nil
}

// PostInit are operations that should be executed against a running Postgres on boot.
func (n *Node) PostInit() error {
	// Ensure local PG is up before establishing connection with
	// consul.
	conn, err := n.NewLocalConnection(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}

	client, err := state.NewConsulClient()
	if err != nil {
		return fmt.Errorf("failed to establish connection with consul: %s", err)
	}

	primaryIP, err := client.CurrentPrimary()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	switch primaryIP {
	case "":
		// Check if we can be a primary
		if !n.validPrimary() {
			return fmt.Errorf("no primary to follow and can't configure self as primary because primary region is '%s' and we are in '%s'", n.Region, os.Getenv("PRIMARY_REGION"))
		}

		// Initialize ourselves as the primary.
		if err := n.createRequiredUsers(conn); err != nil {
			return fmt.Errorf("failed to create required users: %s", err)
		}

		// Creates the replication manager database.
		if _, err := admin.CreateDatabase(conn, n.ManagerDatabaseName, n.ManagerCredentials.Username); err != nil {
			return err
		}

		if err := admin.EnableExtension(conn, "repmgr"); err != nil {
			return err
		}

		if err := registerPrimary(*n); err != nil {
			fmt.Printf("failed to register primary: %s", err)
		}

		// Register ourselves with Consul
		if err := client.RegisterPrimary(n.PrivateIP); err != nil {
			return fmt.Errorf("failed to register primary with consul: %s", err)
		}

		if err := client.RegisterNode(n.ID, n.PrivateIP); err != nil {
			return fmt.Errorf("failed to register member with consul: %s", err)
		}
	case n.PrivateIP:
	// We are an already initialized primary.
	default:
		// If we are here, we are a new node, a standby or a demoted primary who needs
		// to be reconfigured as a standby.
		conn, err := n.NewRepLocalConnection(context.TODO())
		if err != nil {
			return err
		}

		role, err := n.currentRole(context.TODO(), conn)
		if err != nil {
			return err
		}

		if role == primaryRoleName {
			fmt.Println("Unregistering primary")
			if err := unregisterPrimary(*n); err != nil {
				fmt.Printf("failed to unregister primary: %s\n", err)
			}
		}

		if err := registerStandby(*n); err != nil {
			fmt.Printf("failed to register standby: %s\n", err)
		}

		if err := standbyFollow(*n); err != nil {
			fmt.Printf("failed to register standby: %s\n", err)
		}

		// This will noop if the Node has already been registered.
		fmt.Println("Registering Node with Consul")
		if err := client.RegisterNode(n.ID, n.PrivateIP); err != nil {
			return fmt.Errorf("failed to register member with consul: %s", err)
		}
	}

	primaryIP, err = client.CurrentPrimary()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	fmt.Println("Configuring pgbouncer primary")
	if err := n.ConfigurePGBouncerPrimary(primaryIP, false); err != nil {
		return fmt.Errorf("failed to configure pgbouncer primary %s", err)
	}

	return nil
}

func (n *Node) NewPGBouncerConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.ProxyPort))
	return openConnection(ctx, host, "pgbouncer", n.OperatorCredentials)
}

func (n *Node) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.PGPort))
	return openConnection(ctx, host, "postgres", n.OperatorCredentials)
}

func (n *Node) NewRepLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.PGPort))
	return openConnection(ctx, host, "repmgr", n.ManagerCredentials)
}

func (n *Node) NewRepRemoteConnection(ctx context.Context, hostname string) (*pgx.Conn, error) {
	host := net.JoinHostPort(hostname, strconv.Itoa(n.PGPort))
	return openConnection(ctx, host, "repmgr", n.ManagerCredentials)
}

func (n *Node) isInitialized() bool {
	_, err := os.Stat(n.DataDir)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (n *Node) currentRole(ctx context.Context, pg *pgx.Conn) (string, error) {
	return memberRole(ctx, pg, int(n.ID))
}

func (n *Node) validPrimary() bool {
	if n.Region == os.Getenv("PRIMARY_REGION") {
		return true
	}
	return false
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
	if n.isInitialized() {
		return nil
	}

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

func (n *Node) ConfigurePGBouncerAuth() error {
	path := fmt.Sprintf("%s/pgbouncer.auth", "/data")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	contents := fmt.Sprintf("\"%s\" \"%s\"", n.OperatorCredentials.Username, n.OperatorCredentials.Password)
	_, err = file.Write([]byte(contents))
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) ConfigurePGBouncerPrimary(primary string, reload bool) error {
	path := fmt.Sprintf("%s/pgbouncer.database.ini", "/data")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	contents := fmt.Sprintf("[databases]\n* = host=%s port=%d\n", primary, n.PGPort)
	_, err = file.Write([]byte(contents))
	if err != nil {
		return err
	}

	if reload {
		err = n.ReloadPGBouncerConfig()
		if err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	}
	return nil
}

func (n *Node) ReloadPGBouncerConfig() error {
	conn, err := n.NewPGBouncerConnection(context.TODO())
	if err != nil {
		return err
	}
	_, err = conn.Exec(context.TODO(), "RELOAD;")
	if err != nil {
		return err
	}
	return nil
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
	entries := []HBAEntry{
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

func openConnection(ctx context.Context, host string, database string, creds Credentials) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("postgres://%s/%s", host, database)
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
