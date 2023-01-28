package flypg

import (
	"context"
	"encoding/binary"
	"errors"
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
	"github.com/fly-apps/postgres-flex/pkg/utils"
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

	PGBouncer      PGBouncer
	RepMgr         RepMgr
	InternalConfig FlyPGConfig
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
		AppName:            node.AppName,
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

	node.InternalConfig = *NewInternalConfig("/data")

	return node, nil
}

func (n *Node) Init(ctx context.Context) error {
	if err := setDirOwnership(); err != nil {
		return err
	}

	// Attempt to re-introduce zombie node back into the cluster.
	if ZombieLockExists() {
		fmt.Println("Zombie lock file detected. Attempting to rejoin active cluster.")
		zHostname, err := readZombieLock()
		if err != nil {
			return fmt.Errorf("failed to read zombie lock: %s", zHostname)
		}

		if zHostname == "" {
			fmt.Println("Zombie lock does not contain a valid hostname. This means that we were unable to build a consensus on who the real primary is.")
			return fmt.Errorf("unrecoverable zombie state")
		}

		if err := n.RepMgr.rejoinCluster(zHostname); err != nil {
			return fmt.Errorf("failed to rejoin cluster: %s", err)
		}

		if err := removeZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock: %s", err)
		}

		// Ensure the single instance created with the --force-rewind process
		// is cleaned up properly.
		utils.RunCommand("pg_ctl -D /data/postgresql/ stop")
	}

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed initialize cluster state store: %s", err)
	}

	if err := n.configure(ctx, store); err != nil {
		return fmt.Errorf("failed to configure node: %s", err)
	}

	if !n.isPGInitialized() {
		// Check to see if repmgr cluster has been initialized.
		clusterInitialized, err := store.IsInitializationFlagSet()
		if err != nil {
			return fmt.Errorf("failed to verify cluster state %s", err)
		}

		if !clusterInitialized {
			// Initialize ourselves as the primary.
			fmt.Println("Initializing postgres")
			if err := n.initializePG(); err != nil {
				return fmt.Errorf("failed to initialize postgres %s", err)
			}

			fmt.Println("Setting default HBA")
			if err := n.setDefaultHBA(); err != nil {
				return fmt.Errorf("failed updating pg_hba.conf: %s", err)
			}

		} else {
			cloneTarget, err := n.RepMgr.ResolveMemberOverDNS(ctx)
			if err != nil {
				return err
			}

			if err := n.RepMgr.clonePrimary(cloneTarget.Hostname); err != nil {
				return fmt.Errorf("failed to clone primary: %s", err)
			}
		}
	}

	fmt.Println("Initializing Postgres configuration")
	if err := n.configurePostgres(store); err != nil {
		return fmt.Errorf("failed to configure postgres: %s", err)
	}

	return nil
}

// PostInit are operations that should be executed against a running Postgres on boot.
func (n *Node) PostInit(ctx context.Context) error {
	if ZombieLockExists() {
		time.Sleep(30 * time.Second)
		return fmt.Errorf("unable to continue with PostInit while a zombie.  please restart the machine using `fly machine restart %s --app %s`", os.Getenv("FLY_ALLOC_ID"), n.AppName)
	}

	// Ensure local PG is up before establishing connection with consul.
	pgConn, err := n.NewLocalConnection(ctx, "postgres")
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer pgConn.Close(ctx)

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed initialize cluster state store. %v", err)
	}

	clusterInitialized, err := store.IsInitializationFlagSet()
	if err != nil {
		return fmt.Errorf("failed to verify cluster state: %s", err)
	}

	repmgr := n.RepMgr

	// If the cluster has not yet been initialized we should initialize ourself as the primary
	if !clusterInitialized {
		// Check if we can be a primary
		if !repmgr.eligiblePrimary() {
			return fmt.Errorf("no primary to follow and can't configure self as primary because primary region is '%s' and we are in '%s'", os.Getenv("PRIMARY_REGION"), repmgr.Region)
		}

		// Create required users
		if err := n.createRequiredUsers(ctx, pgConn); err != nil {
			return fmt.Errorf("failed to create required users: %s", err)
		}

		// Setup repmgr database and extension
		if err := repmgr.setup(ctx, pgConn); err != nil {
			fmt.Printf("failed to setup repmgr: %s\n", err)
		}

		// Register ourselves as the primary
		if err := repmgr.registerPrimary(); err != nil {
			return fmt.Errorf("failed to register repmgr primary: %s", err)
		}

		// Set flag within consul to let future new members that the cluster exists
		if err := store.SetInitializationFlag(); err != nil {
			return fmt.Errorf("failed to register cluster with consul")
		}

	} else {
		conn, err := repmgr.NewLocalConnection(ctx)
		if err != nil {
			return fmt.Errorf("failed to establish connection to local repmgr: %s", err)
		}
		defer conn.Close(ctx)

		member, err := repmgr.Member(ctx, conn)
		if err != nil {
			// member will not be resolveable if the member has not yet been registered
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("failed to resolve member role: %s", err)
			}
		}

		role := ""
		if member != nil && member.Role != "" {
			role = member.Role
		}
		fmt.Printf("My current role is: %s\n", role)

		switch role {
		case PrimaryRoleName:
			standbys, err := repmgr.StandbyMembers(ctx, conn)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					return fmt.Errorf("failed to query standbys")
				}
			}

			totalMembers := len(standbys) + 1 // include self
			totalActive := 1                  // include self
			totalInactive := 0
			totalConflicts := 0
			conflictMap := map[string]int{}

			// Iterate through each registered standby to confirm that they are up and agree that we
			// are indeed the primary.
			for _, standby := range standbys {
				mConn, err := repmgr.NewRemoteConnection(ctx, standby.Hostname)
				if err != nil {
					fmt.Printf("failed to connect to %s", standby.Hostname)
					totalInactive++
					continue
				}

				primary, err := repmgr.PrimaryMember(ctx, mConn)
				if err != nil {
					fmt.Printf("failed to resolve primary from standby %s", standby.Hostname)
					totalInactive++
					continue
				}

				totalActive++

				if primary.Hostname != n.PrivateIP {
					totalConflicts++
					conflictMap[primary.Hostname]++
				}
			}

			// Using the resolved state metrics, determine if its safe to boot ourself as the primary.
			primary, err := ZombieDiagnosis(n.PrivateIP, totalMembers, totalInactive, totalActive, conflictMap)
			if err != nil {
				if errors.Is(err, ErrZombieDiscovered) {
					fmt.Println("Unable to confirm we are the real primary!")
					fmt.Printf("Registered members: %d, Active member(s): %d, Inactive member(s): %d, Conflicts detected: %d\n",
						totalMembers,
						totalActive,
						totalInactive,
						totalConflicts,
					)

					fmt.Println("Identifying ourself as a Zombie")

					// if primary is non-empty we were able to identify the real primary and should be
					// able to recover on reboot.
					if primary != "" {
						fmt.Printf("Majority of members agree that %s is the real primary\n", primary)
						fmt.Println("Reconfiguring PGBouncer to point to the real primary")
						if err := n.PGBouncer.ConfigurePrimary(ctx, primary, true); err != nil {
							return fmt.Errorf("failed to reconfigure pgbouncer: %s", err)
						}
					}
					// Create a zombie.lock file containing the resolved primary.
					// This will be an empty string if we are unable to resolve the real primary.
					if err := writeZombieLock(primary); err != nil {
						return fmt.Errorf("failed to set zombie lock: %s", err)
					}

					fmt.Println("Setting all existing tables to read-only")
					if err := admin.SetReadOnly(ctx, conn); err != nil {
						return fmt.Errorf("failed to set read-only: %s", err)
					}

					return fmt.Errorf("zombie primary detected. Use `fly machines restart <machine-id>` to rejoin the cluster or consider removing this node")
				}

				return fmt.Errorf("failed to run zombie diagnosis: %s", err)
			}

			// This should never happen, but protect against it just in case.
			if primary != n.PrivateIP {
				return fmt.Errorf("resolved primary '%s' does not match ourself '%s'. this should not happen", primary, n.PrivateIP)
			}

			if err := n.PGBouncer.ConfigurePrimary(ctx, primary, true); err != nil {
				return fmt.Errorf("failed to reconfigure pgbouncer: %s", err)
			}

			if err := admin.UnsetReadOnly(ctx, conn); err != nil {
				return fmt.Errorf("failed to unset read-only")
			}

		default:
			if role != "" {
				fmt.Println("Updating existing standby")
			} else {
				fmt.Println("Registering a new standby")
			}

			if err := repmgr.registerStandby(); err != nil {
				fmt.Printf("failed to register standby: %s\n", err)
			}
		}
	}

	// Reconfigure PGBouncer
	repConn, err := repmgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local repmgr: %s", err)
	}
	defer repConn.Close(ctx)

	if err := n.ReconfigurePGBouncerPrimary(ctx, repConn); err != nil {
		return fmt.Errorf("failed to configure PGBouncer: %s", err)
	}

	return nil
}

func (n *Node) NewLocalConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.Port))
	return openConnection(ctx, host, database, n.OperatorCredentials)
}

func (n *Node) ReconfigurePGBouncerPrimary(ctx context.Context, conn *pgx.Conn) error {
	member, err := n.RepMgr.PrimaryMember(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to find primary: %s", err)
	}

	if err := n.PGBouncer.ConfigurePrimary(ctx, member.Hostname, true); err != nil {
		return fmt.Errorf("failed to configure pgbouncer's primary: %s", err)
	}

	return nil
}

func (n *Node) initializePG() error {
	if n.isPGInitialized() {
		return nil
	}

	if err := ioutil.WriteFile("/data/.default_password", []byte(n.OperatorCredentials.Password), 0644); err != nil {
		return err
	}
	cmd := exec.Command("gosu", "postgres", "initdb", "--pgdata", n.DataDir, "--pwfile=/data/.default_password")
	_, err := cmd.CombinedOutput()

	return err
}

func (n *Node) isPGInitialized() bool {
	_, err := os.Stat(n.DataDir)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (n *Node) configure(ctx context.Context, store *state.Store) error {
	fmt.Println("Initializing internal config")
	if err := n.configureInternal(store); err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("Initializing replication manager")
	if err := n.configureRepmgr(store); err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("Initializing pgbouncer")
	if err := n.configurePGBouncer(store); err != nil {
		fmt.Println(err.Error())
	}

	// Reset PG Primary and wait for primary resolution
	if err := n.PGBouncer.ConfigurePrimary(ctx, "", false); err != nil {
		fmt.Println(err.Error())
	}

	return nil
}

func (n *Node) configureInternal(store *state.Store) error {
	if err := n.InternalConfig.initialize(); err != nil {
		return fmt.Errorf("failed to initialize internal config: %s", err)
	}

	if err := SyncUserConfig(&n.InternalConfig, store); err != nil {
		return fmt.Errorf("failed to sync user config from consul for internal config: %s", err)
	}

	if err := WriteConfigFiles(&n.InternalConfig); err != nil {
		return fmt.Errorf("failed to write config files for internal config: %s", err)
	}

	return nil
}

func (n *Node) configureRepmgr(store *state.Store) error {
	if err := n.RepMgr.initialize(); err != nil {
		return fmt.Errorf("failed to initialize repmgr config: %s", err)
	}

	if err := SyncUserConfig(&n.RepMgr, store); err != nil {
		return fmt.Errorf("failed to sync user config from consul for repmgr: %s", err)
	}

	if err := WriteConfigFiles(&n.RepMgr); err != nil {
		return fmt.Errorf("failed to write config files for repmgr: %s", err)
	}

	return nil
}

func (n *Node) configurePGBouncer(store *state.Store) error {
	if err := n.PGBouncer.initialize(); err != nil {
		return fmt.Errorf("failed to initialize PGBouncer config: %s", err)
	}

	if err := SyncUserConfig(&n.PGBouncer, store); err != nil {
		return fmt.Errorf("failed to sync user config from consul for pgbouncer: %s", err)
	}

	if err := WriteConfigFiles(&n.PGBouncer); err != nil {
		return fmt.Errorf("failed to write config files for pgbouncer: %s", err)
	}

	return nil
}

func (n *Node) configurePostgres(store *state.Store) error {
	if err := n.PGConfig.initialize(); err != nil {
		return fmt.Errorf("failed to initialize pg config: %s", err)
	}

	if err := SyncUserConfig(n.PGConfig, store); err != nil {
		return fmt.Errorf("failed to sync user config from consul for pgbouncer: %s", err.Error())
	}

	if err := WriteConfigFiles(n.PGConfig); err != nil {
		return err
	}

	return nil
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
