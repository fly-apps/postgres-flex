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
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
	"github.com/fly-apps/postgres-flex/pkg/privnet"
	"github.com/jackc/pgx/v4"
)

type Credentials struct {
	Username string
	Password string
}

type Node struct {
	AppName   string
	PrivateIP string
	DataDir   string
	Port      int
	PGConfig  *PGConfig

	SUCredentials       Credentials
	OperatorCredentials Credentials
	ReplCredentials     Credentials

	PGBouncer PGBouncer
	RepMgr    RepMgr
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName: "local",
		Port:    5433,
		DataDir: "/data/postgresql",
	}

	if appName := os.Getenv("FLY_APP_NAME"); appName != "" {
		node.AppName = appName
	}

	ipv6, err := privnet.PrivateIPv6()
	if err != nil {
		return nil, fmt.Errorf("failed getting private ip: %s", err)
	}
	node.PrivateIP = ipv6.String()

	if port, err := strconv.Atoi(os.Getenv("PG_PORT")); err == nil {
		node.Port = port
	}

	// Stub configuration
	node.PGConfig = NewConfig(node.DataDir)

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

	node.ReplCredentials = Credentials{
		Username: "repmgr",
		Password: os.Getenv("REPL_PASSWORD"),
	}

	node.PGBouncer = PGBouncer{
		PrivateIP:   node.PrivateIP,
		Port:        5432,
		ForwardPort: 5433,
		ConfigPath:  "/data/pgbouncer",
		Credentials: node.OperatorCredentials,
	}

	// Generate a random, reconstructable signed int32
	machineID := os.Getenv("FLY_ALLOC_ID")
	seed := binary.LittleEndian.Uint64([]byte(machineID))
	rand.Seed(int64(seed))

	node.RepMgr = RepMgr{
		ID:                 rand.Int31(),
		Region:             os.Getenv("FLY_REGION"),
		ConfigPath:         "/data/repmgr.conf",
		InternalConfigPath: "/data/repmgr.internal.conf",
		UserConfigPath:     "/data/repmgr.user.conf",
		DataDir:            node.DataDir,
		PrivateIP:          node.PrivateIP,
		Port:               5433,
		DatabaseName:       "repmgr",
		Credentials:        node.ReplCredentials,
	}

	return node, nil
}

func (n *Node) Init(ctx context.Context) error {
	if err := setDirOwnership(); err != nil {
		return err
	}

	cs, err := state.NewClusterState()
	if err != nil {
		return fmt.Errorf("failed initialize cluster state store. %v", err)
	}

	primary, err := cs.PrimaryMember()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	repmgr := n.RepMgr
	pgbouncer := n.PGBouncer
	PGConfig := n.PGConfig

	fmt.Println("Initializing replication manager")
	if err := repmgr.initialize(); err != nil {
		fmt.Printf("Failed to initialize repmgr: %s\n", err.Error())
	}

	err = SyncUserConfig(&repmgr, cs.Store)
	if err != nil {
		fmt.Printf("Failed to sync user config from consul for repmgr: %s\n", err.Error())
	}

	err = WriteConfigFiles(&repmgr)
	if err != nil {
		fmt.Printf("Failed to write config files for repmgr: %s\n", err.Error())
	}

	fmt.Println("Initializing pgbouncer")
	if err := pgbouncer.initialize(); err != nil {
		return err
	}

	err = SyncUserConfig(&pgbouncer, cs.Store)
	if err != nil {
		fmt.Printf("Failed to sync user config from consul for pgbouncer: %s\n", err.Error())
	}

	err = WriteConfigFiles(&pgbouncer)
	if err != nil {
		fmt.Printf("Failed to write config files for pgbouncer: %s\n", err.Error())
	}

	switch {
	case primary == nil:
		// Initialize ourselves as the primary.
		fmt.Println("Initializing postgres")
		if err := n.initialize(); err != nil {
			return fmt.Errorf("failed to initialize postgres %s", err)
		}

		fmt.Println("Setting default HBA")
		if err := n.setDefaultHBA(); err != nil {
			return fmt.Errorf("failed updating pg_hba.conf: %s", err)
		}
	case primary.Hostname == n.PrivateIP:
		// noop
	default:
		// If we are here we are either a standby, new node or primary coming back from the dead.
		clonePrimary := true
		if n.isInitialized() {
			// Attempt to resolve our role by querying the primary.
			remoteConn, err := repmgr.NewRemoteConnection(ctx, primary.Hostname)
			if err != nil {
				return fmt.Errorf("failed to resolve my role according to the primary: %s", err)
			}
			defer remoteConn.Close(ctx)

			role, err := repmgr.memberRoleByHostname(ctx, remoteConn, n.PrivateIP)
			if err != nil {
				return fmt.Errorf("failed to resolve role for %s: %s", primary.Hostname, err)
			}

			fmt.Printf("My role is: %s\n", role)
			if role == StandbyRoleName {
				clonePrimary = false
			}
		}

		if clonePrimary {
			fmt.Println("Cloning from primary")
			if err := repmgr.clonePrimary(primary.Hostname); err != nil {
				return fmt.Errorf("failed to clone primary: %s", err)
			}
		}
	}

	fmt.Println("Resolving PG configuration settings.")
	PGConfig.Setup()

	err = SyncUserConfig(PGConfig, cs.Store)
	if err != nil {
		fmt.Printf("Failed to sync user config from consul for pgbouncer: %s\n", err.Error())
	}

	WriteConfigFiles(PGConfig)

	PGConfig.Print(os.Stdout)

	return nil
}

// PostInit are operations that should be executed against a running Postgres on boot.
func (n *Node) PostInit(ctx context.Context) error {
	// Ensure local PG is up before establishing connection with consul.
	conn, err := n.NewLocalConnection(ctx, "postgres")
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer conn.Close(ctx)

	cs, err := state.NewClusterState()
	if err != nil {
		return fmt.Errorf("failed initialize cluster state store. %v", err)
	}

	primary, err := cs.PrimaryMember()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	repmgr := n.RepMgr
	pgbouncer := n.PGBouncer

	switch {
	case primary == nil:
		// Check if we can be a primary
		if !repmgr.eligiblePrimary() {
			return fmt.Errorf("no primary to follow and can't configure self as primary because primary region is '%s' and we are in '%s'", os.Getenv("PRIMARY_REGION"), repmgr.Region)
		}

		// Create required users
		if err := n.createRequiredUsers(ctx, conn); err != nil {
			return fmt.Errorf("failed to create required users: %s", err)
		}

		// Setup repmgr database, extension, and register ourselves as the primary
		fmt.Println("Performing Repmgr setup")
		if err := repmgr.setup(ctx, conn); err != nil {
			fmt.Printf("failed to setup repmgr: %s\n", err)
		}

		// Register primary member with consul
		fmt.Println("Registering member")
		if err := cs.RegisterMember(repmgr.ID, n.PrivateIP, repmgr.Region, true); err != nil {
			return fmt.Errorf("failed to register member with consul: %s", err)
		}

	case primary.Hostname == n.PrivateIP:
		// Re-register the primary in order to pick up any changes made to the configuration file.
		fmt.Println("Updating primary record")
		if err := repmgr.registerPrimary(); err != nil {
			fmt.Printf("failed to register primary with repmgr: %s", err)
		}
	default:
		// If we are here we are a new node, standby or a demoted primary who needs to be reconfigured as a standby.
		// Attempt to resolve our role from repmgr
		conn, err := repmgr.NewLocalConnection(ctx)
		if err != nil {
			return err
		}
		defer conn.Close(ctx)

		role, err := repmgr.CurrentRole(ctx, conn)
		if err != nil {
			return err
		}

		// If we are a primary coming back from the dead, make sure we unregister ourselves as primary.
		if role == PrimaryRoleName {
			fmt.Println("Unregistering primary")
			if err := repmgr.unregisterPrimary(); err != nil {
				fmt.Printf("failed to unregister primary: %s\n", err)
			}
		}

		fmt.Println("Registering standby")
		if err := repmgr.registerStandby(); err != nil {
			fmt.Printf("failed to register standby: %s\n", err)
		}

		if err := repmgr.followPrimary(); err != nil {
			fmt.Printf("failed to register standby: %s\n", err)
		}

		// Register member with consul if it hasn't been already
		if err := cs.RegisterMember(repmgr.ID, n.PrivateIP, repmgr.Region, false); err != nil {
			return fmt.Errorf("failed to register member with consul: %s", err)
		}
	}
	// Requery the primaryIP from consul in case the primary was assigned above.
	primary, err = cs.PrimaryMember()
	if err != nil {
		return fmt.Errorf("failed to query current primary: %s", err)
	}

	if err := pgbouncer.ConfigurePrimary(ctx, primary.Hostname, true); err != nil {
		return fmt.Errorf("failed to configure pgbouncer's primary: %s", err)
	}

	return nil
}

func (n *Node) NewLocalConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.Port))
	return openConnection(ctx, host, database, n.OperatorCredentials)
}

func (n *Node) isInitialized() bool {
	_, err := os.Stat(n.DataDir)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (n *Node) initialize() error {
	if n.isInitialized() {
		return nil
	}

	if err := ioutil.WriteFile("/data/.default_password", []byte(n.OperatorCredentials.Password), 0644); err != nil {
		return err
	}
	cmd := exec.Command("gosu", "postgres", "initdb", "--pgdata", n.DataDir, "--pwfile=/data/.default_password")
	_, err := cmd.CombinedOutput()

	return err
}

func (n *Node) createRequiredUsers(ctx context.Context, conn *pgx.Conn) error {
	curUsers, err := admin.ListUsers(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to list existing users: %s", err)
	}

	credMap := map[string]string{
		n.SUCredentials.Username:       n.SUCredentials.Password,
		n.OperatorCredentials.Username: n.OperatorCredentials.Password,
		n.ReplCredentials.Username:     n.ReplCredentials.Password,
	}

	for user, pass := range credMap {
		exists := false
		for _, curUser := range curUsers {
			if user == curUser.Username {
				exists = true
			}
		}

		if exists {
			if err := admin.ChangePassword(ctx, conn, user, pass); err != nil {
				return fmt.Errorf("failed to update credentials for user %s: %s", user, err)
			}
		} else {
			if err := admin.CreateUser(ctx, conn, user, pass); err != nil {
				return fmt.Errorf("failed to create require user %s: %s", user, err)
			}

			if err := admin.GrantSuperuser(ctx, conn, user); err != nil {
				return fmt.Errorf("failed to grant superuser privileges to user %s: %s", user, err)
			}
		}
	}

	return nil
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
			Database: "all",
			User:     "flypgadmin",
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: n.RepMgr.DatabaseName,
			User:     n.RepMgr.Credentials.Username,
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: "replication",
			User:     n.RepMgr.Credentials.Username,
			Method:   "trust",
		},
		{
			Type:     "host",
			Database: "replication",
			User:     n.RepMgr.Credentials.Username,
			Address:  "::0/0",
			Method:   "trust",
		},
		{
			Type:     "host",
			Database: n.RepMgr.DatabaseName,
			User:     n.RepMgr.Credentials.Username,
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

func UnregisterMemberByHostname(ctx context.Context, hostname string) error {
	cs, err := state.NewClusterState()
	if err != nil {
		fmt.Printf("failed initialize cluster state store. %v", err)
	}

	member, err := cs.FindMemberByHostname(hostname)
	if err != nil {
		return err
	}

	if member == nil {
		return state.ErrMemberNotFound
	}

	node, err := NewNode()
	if err != nil {
		return err
	}

	// Unregister from repmgr
	err = node.RepMgr.UnregisterStandby(int(member.ID))
	if err != nil {
		return fmt.Errorf("failed to unregister member %d from repmgr: %s", member.ID, err)
	}

	// Unregister from consul
	if err := cs.UnregisterMember(member.ID); err != nil {
		return fmt.Errorf("failed to unregister member %d from consul: %v", member.ID, err)
	}

	return nil
}

func UnregisterMemberByID(ctx context.Context, id int32) error {

	cs, err := state.NewClusterState()
	if err != nil {
		fmt.Printf("failed initialize cluster state store. %v", err)
	}

	member, err := cs.FindMemberByID(id)
	if err != nil {
		return err
	}

	if member == nil {
		return state.ErrMemberNotFound
	}

	node, err := NewNode()
	if err != nil {
		return err
	}

	// Unregister from repmgr
	err = node.RepMgr.UnregisterStandby(int(member.ID))
	if err != nil {
		return fmt.Errorf("failed to unregister member %d from repmgr: %s", member.ID, err)
	}

	// Unregister from consul
	if err := cs.UnregisterMember(member.ID); err != nil {
		return fmt.Errorf("failed to unregister member %d from consul: %v", member.ID, err)
	}

	return nil
}

func openConnection(parentCtx context.Context, host string, database string, creds Credentials) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("postgres://%s/%s", host, database)
	conf, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	conf.User = creds.Username
	conf.Password = creds.Password
	conf.ConnectTimeout = 5 * time.Second

	return pgx.ConnectConfig(ctx, conf)
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
	return err
}
