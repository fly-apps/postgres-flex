package flypg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/fly-apps/postgres-flex/internal/privnet"
	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
)

type Node struct {
	AppName       string
	PrivateIP     string
	PrimaryRegion string
	DataDir       string
	Port          int

	SUCredentials       admin.Credential
	OperatorCredentials admin.Credential
	ReplCredentials     admin.Credential

	PGConfig  PGConfig
	RepMgr    RepMgr
	FlyConfig FlyPGConfig
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

	node.RepMgr = RepMgr{
		AppName:            node.AppName,
		PrimaryRegion:      node.PrimaryRegion,
		Region:             os.Getenv("FLY_REGION"),
		ConfigPath:         "/data/repmgr.conf",
		InternalConfigPath: "/data/repmgr.internal.conf",
		UserConfigPath:     "/data/repmgr.user.conf",
		PasswordConfigPath: "/data/.pgpass",
		DataDir:            node.DataDir,
		PrivateIP:          node.PrivateIP,
		Port:               5433,
		DatabaseName:       "repmgr",
		Credentials:        node.ReplCredentials,
	}

	_, present := os.LookupEnv("WITNESS")
	node.RepMgr.Witness = present

	node.PGConfig = PGConfig{
		AppName:                node.AppName,
		DataDir:                node.DataDir,
		Port:                   node.Port,
		ConfigFilePath:         fmt.Sprintf("%s/postgresql.conf", node.DataDir),
		InternalConfigFilePath: fmt.Sprintf("%s/postgresql.internal.conf", node.DataDir),
		UserConfigFilePath:     fmt.Sprintf("%s/postgresql.user.conf", node.DataDir),

		passwordFilePath: "/data/.default_password",
		repmgrUsername:   node.RepMgr.Credentials.Username,
		repmgrDatabase:   node.RepMgr.DatabaseName,
	}

	node.FlyConfig = FlyPGConfig{
		internalConfigFilePath: "/data/flypg.internal.conf",
		userConfigFilePath:     "/data/flypg.user.conf",
	}

	return node, nil
}

func (n *Node) Init(ctx context.Context) error {
	// Ensure directory and files have proper permissions
	if err := setDirOwnership(); err != nil {
		return fmt.Errorf("failed to set directory ownership: %s", err)
	}

	// Snapshot restore
	if os.Getenv("FLY_RESTORED_FROM") != "" {
		active, err := isRestoreActive()
		if err != nil {
			return fmt.Errorf("failed to verify active restore: %s", err)
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
			return fmt.Errorf("failed to handle zombie lock: %s", err)
		}
	}

	err := WriteSSHKey()
	if err != nil {
		return fmt.Errorf("failed write ssh keys: %s", err)
	}

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed initialize cluster state store: %s", err)
	}

	if err := n.RepMgr.initialize(); err != nil {
		return fmt.Errorf("failed to initialize repmgr: %s", err)
	}

	if !n.PGConfig.isInitialized() {
		// Check to see if cluster has already been initialized.
		clusterInitialized, err := store.IsInitializationFlagSet()
		if err != nil {
			return fmt.Errorf("failed to verify cluster state %s", err)
		}

		if clusterInitialized {
			if n.RepMgr.Witness {
				log.Println("Provisioning witness")
				if err := n.PGConfig.writePasswordFile(n.OperatorCredentials.Password); err != nil {
					return fmt.Errorf("failed to write pg password file: %s", err)
				}

				if err := n.PGConfig.initdb(); err != nil {
					return fmt.Errorf("failed to initialize postgres %s", err)
				}
			} else {
				log.Println("Provisioning standby")
				cloneTarget, err := n.RepMgr.ResolveMemberOverDNS(ctx)
				if err != nil {
					return fmt.Errorf("failed to resolve member over dns: %s", err)
				}

				if err := n.RepMgr.clonePrimary(cloneTarget.Hostname); err != nil {
					// Clean-up the directory so it can be retried.
					if rErr := os.Remove(n.DataDir); rErr != nil {
						log.Printf("[ERROR] failed to cleanup postgresql dir after clone error: %s\n", rErr)
					}

					return fmt.Errorf("failed to clone primary: %s", err)
				}
			}
		} else {
			log.Println("Provisioning primary")
			if err := n.PGConfig.writePasswordFile(n.OperatorCredentials.Password); err != nil {
				return fmt.Errorf("failed to write pg password file: %s", err)
			}

			if err := n.PGConfig.initdb(); err != nil {
				return fmt.Errorf("failed to initialize postgres %s", err)
			}
		}
	}

	if err := n.FlyConfig.initialize(); err != nil {
		return fmt.Errorf("failed to initialize fly config: %s", err)
	}

	if err := n.PGConfig.initialize(store); err != nil {
		return fmt.Errorf("failed to initialize pg config: %s", err)
	}

	if err := setDirOwnership(); err != nil {
		return fmt.Errorf("failed to set directory ownership: %s", err)
	}

	return nil
}

// PostInit are operations that need to be executed against a running Postgres on boot.
func (n *Node) PostInit(ctx context.Context) error {
	if ZombieLockExists() {
		log.Println("[ERROR] Manual intervention required.")
		log.Println("[ERROR] If a new primary has been established, consider adding a new replica with `fly machines clone <primary-machine-id>` and then remove this member.")
		log.Println("[ERROR] Sleeping for 5 minutes.")
		time.Sleep(5 * time.Minute)
		return fmt.Errorf("unrecoverable zombie")
	}

	// Use the Postgres user on boot, since our internal user may not have been created yet.
	conn, err := n.NewLocalConnection(ctx, "postgres", n.OperatorCredentials)
	if err != nil {
		// Check to see if this is an authentication error.
		if strings.Contains(err.Error(), "28P01") {
			log.Println("[WARN] `postgres` user password does not match the `OPERATOR_PASSWORD` secret")
			log.Printf("[WARN] Use `fly secrets set OPERATOR_PASSWORD=<password> --app %s` to resolve the issue\n", n.AppName)
		}

		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Check to see if we have already been registered with repmgr.
	registered, err := isRegistered(ctx, conn, n)
	if err != nil {
		return fmt.Errorf("failed to verify member registration: %s", err)
	}

	if registered {
		repConn, err := n.RepMgr.NewLocalConnection(ctx)
		if err != nil {
			return fmt.Errorf("failed to establish connection to local repmgr: %s", err)
		}
		defer func() { _ = repConn.Close(ctx) }()

		// Existing member
		member, err := n.RepMgr.Member(ctx, repConn)
		if err != nil {
			return fmt.Errorf("failed to resolve member role: %s", err)
		}

		// Restart repmgrd in the event the IP changes for an already registered node.
		// This can happen if the underlying volume is moved to a different node.
		daemonRestartRequired := n.RepMgr.daemonRestartRequired(member)

		switch member.Role {
		case PrimaryRoleName:
			// Verify cluster state to ensure we are the actual primary and not a zombie.
			primary, err := PerformScreening(ctx, repConn, n)
			if errors.Is(err, ErrZombieDiagnosisUndecided) {
				log.Println("[ERROR] Unable to confirm that we are the true primary!")
				// Turn member read-only
				if err := Quarantine(ctx, n, primary); err != nil {
					return fmt.Errorf("failed to quarantine failed primary: %s", err)
				}
			} else if errors.Is(err, ErrZombieDiscovered) {
				log.Printf("[ERROR] The majority of registered members agree that '%s' is the real primary.\n", primary)
				// Turn member read-only
				if err := Quarantine(ctx, n, primary); err != nil {
					return fmt.Errorf("failed to quarantine failed primary: %s", err)
				}

				panic(err)
			} else if err != nil {
				return fmt.Errorf("failed to run zombie diagnosis: %s", err)
			}

			// This should never happen
			if primary != n.PrivateIP {
				return fmt.Errorf("resolved primary '%s' does not match ourself '%s'. this should not happen",
					primary,
					n.PrivateIP,
				)
			}

			// Re-register primary to apply any configuration changes.
			if err := n.RepMgr.registerPrimary(daemonRestartRequired); err != nil {
				return fmt.Errorf("failed to re-register existing primary: %s", err)
			}

			// Readonly lock is set when disk capacity is dangerously high.
			if !ReadOnlyLockExists() {
				if err := BroadcastReadonlyChange(ctx, n, false); err != nil {
					return fmt.Errorf("failed to unset read-only: %s", err)
				}
			}
		case StandbyRoleName:
			// Register existing standby to apply any configuration changes.
			if err := n.RepMgr.registerStandby(daemonRestartRequired); err != nil {
				return fmt.Errorf("failed to register existing standby: %s", err)
			}
		case WitnessRoleName:
			primary, err := n.RepMgr.PrimaryMember(ctx, repConn)
			if err != nil {
				return fmt.Errorf("failed to resolve primary member when updating witness: %s", err)
			}

			// Register existing witness to apply any configuration changes.
			if err := n.RepMgr.registerWitness(primary.Hostname); err != nil {
				return fmt.Errorf("failed to register existing witness: %s", err)
			}
		default:
			return fmt.Errorf("member has unknown role: %q", member.Role)
		}

		// Ensure connection is closed.
		if err := repConn.Close(ctx); err != nil {
			return fmt.Errorf("failed to close connection: %s", err)
		}
	} else {
		// New member

		// Check with consul to see if the cluster has already been initialized
		store, err := state.NewStore()
		if err != nil {
			return fmt.Errorf("failed initialize cluster state store. %v", err)
		}

		// The initialization flag is set after the primary is registered.
		clusterInitialized, err := store.IsInitializationFlagSet()
		if err != nil {
			return fmt.Errorf("failed to verify cluster state: %s", err)
		}

		if !clusterInitialized {
			// Configure as primary
			log.Println("Registering primary")

			// Verify we reside within the clusters primary region
			if !n.RepMgr.eligiblePrimary() {
				return fmt.Errorf("unable to configure as the primary. expected region: %q, got: %q",
					n.PrimaryRegion,
					n.RepMgr.Region,
				)
			}

			// Create required users
			if err := n.setupCredentials(ctx, conn); err != nil {
				return fmt.Errorf("failed to create required users: %s", err)
			}

			// Setup repmgr database and extension
			if err := n.RepMgr.enable(ctx, conn); err != nil {
				return fmt.Errorf("failed to enable repmgr: %s", err)
			}

			// Register ourself as the primary
			if err := n.RepMgr.registerPrimary(false); err != nil {
				return fmt.Errorf("failed to register repmgr primary: %s", err)
			}

			// Set initialization flag within consul so future members know they are joining
			// an existing cluster.
			if err := store.SetInitializationFlag(); err != nil {
				return fmt.Errorf("failed to register cluster with consul")
			}

			// Let the boot process know that we've already been configured.
			if err := issueRegistrationCert(); err != nil {
				return fmt.Errorf("failed to issue registration certificate: %s", err)
			}
		} else {
			if n.RepMgr.Witness {
				log.Println("Registering witness")

				// Create required users
				if err := n.setupCredentials(ctx, conn); err != nil {
					return fmt.Errorf("failed to create required users: %s", err)
				}

				// Setup repmgr database and extension
				if err := n.RepMgr.enable(ctx, conn); err != nil {
					return fmt.Errorf("failed to enable repmgr: %s", err)
				}

				primary, err := n.RepMgr.ResolveMemberOverDNS(ctx)
				if err != nil {
					return fmt.Errorf("failed to resolve primary member: %s", err)
				}

				if err := n.RepMgr.registerWitness(primary.Hostname); err != nil {
					return fmt.Errorf("failed to register witness: %s", err)
				}
			} else {
				log.Println("Registering standby")
				if err := n.RepMgr.registerStandby(false); err != nil {
					return fmt.Errorf("failed to register new standby: %s", err)
				}
			}

			// Let the boot process know that we've already been configured.
			if err := issueRegistrationCert(); err != nil {
				return fmt.Errorf("failed to issue registration certificate: %s", err)
			}
		}
	}

	// Ensure connection is closed.
	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	return nil
}

func (n *Node) setupCredentials(ctx context.Context, conn *pgx.Conn) error {
	requiredCredentials := []admin.Credential{
		n.OperatorCredentials,
		n.ReplCredentials,
		n.SUCredentials,
	}

	return admin.ManageDefaultUsers(ctx, conn, requiredCredentials)
}

// NewLocalConnection opens up a new connection using the flypgadmin user.
func (n *Node) NewLocalConnection(ctx context.Context, database string, creds admin.Credential) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP, strconv.Itoa(n.Port))
	return openConnection(ctx, host, database, creds)
}

func openConnection(parentCtx context.Context, host string, database string, creds admin.Credential) (*pgx.Conn, error) {
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
