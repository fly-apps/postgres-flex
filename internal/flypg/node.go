package flypg

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/fly-apps/postgres-flex/internal/privnet"
	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
)

type Credentials struct {
	Username string
	Password string
}

type Node struct {
	AppName       string
	PrivateIP     string
	PrimaryRegion string
	DataDir       string
	Port          int
	PGConfig      *PGConfig

	SUCredentials       Credentials
	OperatorCredentials Credentials
	ReplCredentials     Credentials

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

	node.PrimaryRegion = os.Getenv("PRIMARY_REGION")
	if node.PrimaryRegion == "" {
		return nil, fmt.Errorf("PRIMARY_REGION environment variable must be set")
	}

	if port, err := strconv.Atoi(os.Getenv("PG_PORT")); err == nil {
		node.Port = port
	}

	// Stub configuration
	node.PGConfig = NewConfig(node.DataDir, node.Port)

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

	// Generate a random, reconstructable signed int32
	machineID := os.Getenv("FLY_ALLOC_ID")
	seed := binary.LittleEndian.Uint64([]byte(machineID))
	rand.Seed(int64(seed))

	node.RepMgr = RepMgr{
		ID:                 rand.Int31(),
		AppName:            node.AppName,
		PrimaryRegion:      node.PrimaryRegion,
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
	// Ensure directory and files have proper permissions
	if err := setDirOwnership(); err != nil {
		return err
	}

	// Check to see if we were just restored
	if os.Getenv("FLY_RESTORED_FROM") != "" {
		// Check to see if there's an active restore.
		active, err := isRestoreActive()
		if err != nil {
			return err
		}

		if active {
			if err := Restore(ctx, n); err != nil {
				return fmt.Errorf("failed to issue restore: %s", err)
			}
		}
	}

	// Verify whether we are a booting zombie.
	if ZombieLockExists() {
		if err := handleZombieLock(ctx, n); err != nil {
			return err
		}
	}

	err := writeSSHKey()
	if err != nil {
		return fmt.Errorf("failed initialize ssh. %v", err)
	}

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed initialize cluster state store: %s", err)
	}

	if err := n.configureInternal(store); err != nil {
		return fmt.Errorf("failed to set internal config: %s", err)
	}

	if err := n.configureRepmgr(store); err != nil {
		return fmt.Errorf("failed to configure repmgr config: %s", err)
	}

	if !n.isPGInitialized() {
		// Check to see if cluster has already been initialized.
		clusterInitialized, err := store.IsInitializationFlagSet()
		if err != nil {
			return fmt.Errorf("failed to verify cluster state %s", err)
		}

		if !clusterInitialized {
			fmt.Println("Provisioning primary")

			// Initialize ourselves as the primary.
			if err := n.initializePG(); err != nil {
				return fmt.Errorf("failed to initialize postgres %s", err)
			}

			if err := n.setDefaultHBA(); err != nil {
				return fmt.Errorf("failed updating pg_hba.conf: %s", err)
			}

		} else {
			fmt.Println("Provisioning standby")
			// Initialize ourselves as a standby
			cloneTarget, err := n.RepMgr.ResolveMemberOverDNS(ctx)
			if err != nil {
				return err
			}

			if err := n.RepMgr.clonePrimary(cloneTarget.Hostname); err != nil {
				return fmt.Errorf("failed to clone primary: %s", err)
			}
		}
	}

	if err := n.configurePostgres(store); err != nil {
		return fmt.Errorf("failed to configure postgres: %s", err)
	}

	if err := setDirOwnership(); err != nil {
		return err
	}

	return nil
}

// PostInit are operations that should be executed against a running Postgres on boot.
func (n *Node) PostInit(ctx context.Context) error {
	if ZombieLockExists() {
		fmt.Println("Manual intervention required. Delete the zombie.lock file and restart the machine to force a retry.")
		fmt.Println("Sleeping for 5 minutes.")
		time.Sleep(5 * time.Minute)

		return fmt.Errorf("unrecoverable zombie")
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
			return fmt.Errorf("no primary to follow and can't configure self as primary because primary region is '%s' and we are in '%s'", n.PrimaryRegion, repmgr.Region)
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

		// Set initialization flag within consul so future members know they are joining
		// an existing cluster.
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
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("failed to resolve member role: %s", err)
			}
		}

		role := ""
		if member != nil {
			role = member.Role
		}

		switch role {
		case PrimaryRoleName:
			primary, err := PerformScreening(ctx, conn, n)
			if errors.Is(err, ErrZombieDiagnosisUndecided) {
				fmt.Println("Unable to confirm that we are the true primary!")
				if err := Quarantine(ctx, conn, n, primary); err != nil {
					return fmt.Errorf("failed to quarantine failed primary: %s", err)
				}
			} else if errors.Is(err, ErrZombieDiscovered) {
				fmt.Printf("The majority of registered members agree that '%s' is the real primary.\n", primary)

				if err := Quarantine(ctx, conn, n, primary); err != nil {
					return fmt.Errorf("failed to quarantine failed primary: %s", err)
				}
				// Issue panic to force a process restart so we can attempt to rejoin
				// the the cluster we've diverged from.
				panic(err)
			} else if err != nil {
				return fmt.Errorf("failed to run zombie diagnosis: %s", err)
			}

			// This should never happen, but protect against it just in case.
			if primary != n.PrivateIP {
				return fmt.Errorf("resolved primary '%s' does not match ourself '%s'. this should not happen", primary, n.PrivateIP)
			}

			// Readonly lock is set by healthchecks when disk capacity is dangerously high.
			if !ReadOnlyLockExists() {
				if err := BroadcastReadonlyChange(ctx, n, false); err != nil {
					return fmt.Errorf("failed to unset read-only: %s", err)
				}
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

	return nil
}

func (n *Node) NewLocalConnection(ctx context.Context, database string) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.Port))
	return openConnection(ctx, host, database, n.OperatorCredentials)
}

func (n *Node) initializePG() error {
	if n.isPGInitialized() {
		return nil
	}

	if err := os.WriteFile("/data/.default_password", []byte(n.OperatorCredentials.Password), 0644); err != nil {
		return err
	}
	cmd := exec.Command("gosu", "postgres", "initdb", "--pgdata", n.DataDir, "--pwfile=/data/.default_password")
	if _, err := cmd.CombinedOutput(); err != nil {
		return err
	}

	return nil
}

func (n *Node) isPGInitialized() bool {
	_, err := os.Stat(n.DataDir)
	if os.IsNotExist(err) {
		return false
	}
	return true
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

func (n *Node) configurePostgres(store *state.Store) error {
	if err := n.PGConfig.initialize(); err != nil {
		return fmt.Errorf("failed to initialize pg config: %s", err)
	}

	if err := SyncUserConfig(n.PGConfig, store); err != nil {
		return fmt.Errorf("failed to sync user config from consul for postgres: %s", err.Error())
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
	defer file.Close()

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
	pgUID, pgGID, err := utils.SystemUserIDs("postgres")
	if err != nil {
		return fmt.Errorf("failed to find postgres user ids: %s", err)
	}

	cmdStr := fmt.Sprintf("chown -R %d:%d %s", pgUID, pgGID, "/data")
	cmd := exec.Command("sh", "-c", cmdStr)
	if _, err = cmd.Output(); err != nil {
		return err
	}

	return nil
}
