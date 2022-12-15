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
	Port      int

	PGBouncer PGBouncer

	SUCredentials       Credentials
	OperatorCredentials Credentials

	ManagerCredentials  Credentials
	ManagerConfigPath   string
	ManagerDatabaseName string
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName:             "local",
		Port:                5433,
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
		node.Port = port
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

	node.PGBouncer = PGBouncer{
		PrivateIP:   node.PrivateIP,
		Port:        5432,
		ConfigPath:  "/data/pgbouncer",
		Credentials: node.SUCredentials,
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

	// Initialize PGBouncer
	if err := n.PGBouncer.configure(primaryIP); err != nil {
		return err
	}

	// Writes or updates the replication manager configuration.
	if err := initializeRepmgr(*n); err != nil {
		fmt.Printf("Failed to initialize replmgr: %s\n", err.Error())
	}

	switch primaryIP {
	case n.PrivateIP:
		// Noop
	case "":
		// Initialize ourselves as the primary.
		fmt.Println("Initializing postgres")
		if err := n.initializePostgres(); err != nil {
			return fmt.Errorf("failed to initialize postgres %s", err)
		}

		fmt.Println("Setting default HBA")
		if err := n.setDefaultHBA(); err != nil {
			return fmt.Errorf("failed updating pg_hba.conf: %s", err)
		}
	default:
		// If we are here we are either a standby, new node or primary coming back from the dead.
		clonePrimary := true
		if n.isInitialized() {
			// Attempt to resolve our role by querying the primary.
			remoteConn, err := n.NewRepRemoteConnection(context.TODO(), primaryIP)
			if err != nil {
				return fmt.Errorf("failed to resolve my role according to the primary: %s", err)
			}
			role, err := memberRoleByHostname(context.TODO(), remoteConn, n.PrivateIP)
			if err != nil {
				return fmt.Errorf("failed to resolve role for %s: %s", primaryIP, err)
			}

			fmt.Printf("My role is: %s\n", role)
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
	if err := n.PGBouncer.configureAuth(); err != nil {
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
	case n.PrivateIP:
		// Re-register the primary in order to pick up any changes made to the
		// configuration file.
		fmt.Println("Updating primary record")
		if err := registerPrimary(*n); err != nil {
			fmt.Printf("failed to register primary: %s", err)
		}
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
	if err := n.PGBouncer.ConfigurePrimary(primaryIP, false); err != nil {
		return fmt.Errorf("failed to configure pgbouncer primary %s", err)
	}

	return nil
}

func (n *Node) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.Port))
	return openConnection(ctx, host, "postgres", n.SUCredentials)
}

func (n *Node) NewRepLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.Port))
	return openConnection(ctx, host, "repmgr", n.ManagerCredentials)
}

func (n *Node) NewRepRemoteConnection(ctx context.Context, hostname string) (*pgx.Conn, error) {
	host := net.JoinHostPort(hostname, strconv.Itoa(n.Port))
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
