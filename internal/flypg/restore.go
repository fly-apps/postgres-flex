package flypg

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/supervisor"
	"github.com/jackc/pgx/v5"
)

const (
	pathToHBAFile   = "/data/postgresql/pg_hba.conf"
	pathToHBABackup = "/data/postgresql/pg_hba.conf.bak"
	postmasterPath  = "/data/postgresql/postmaster.pid"
	restoreLockFile = "/data/restore.lock"
)

// prepareRemoteRestore will reset the environment to a state where it can be restored
// from a remote backup. This process includes:
// * Clearing any locks that may have been set on the original cluster.
// * Dropping the repmgr database to clear any metadata that belonged to the old cluster.
// * Ensuring the internal user credentials match the environment.
func prepareRemoteRestore(ctx context.Context, node *Node) error {
	// Clear any locks that may have been set on the original cluster
	if err := clearLocks(); err != nil {
		return fmt.Errorf("failed to clear locks: %s", err)
	}

	// create a copy of the pg_hba.conf file so we can revert back to it when needed.
	// If the file doesn't exist, then we are new standby coming up.
	if err := backupHBAFile(); err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("failed backing up pg_hba.conf: %s", err)
	}

	if err := grantLocalAccess(); err != nil {
		return fmt.Errorf("failed to grant local access: %s", err)
	}

	// Clear the standby.signal if it exists.
	if _, err := os.Stat("/data/postgresql/standby.signal"); err == nil {
		log.Println("Restoring from a hot standby.")
		// Clear the signal so we can boot.
		if err = os.Remove("/data/postgresql/standby.signal"); err != nil {
			return fmt.Errorf("failed to remove standby signal: %s", err)
		}
	}

	// Boot postgres in standalone mode
	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("postgres", fmt.Sprintf("gosu postgres postgres -D /data/postgresql -p 5433 -h %s", node.PrivateIP))

	go func() {
		if err := svisor.Run(); err != nil {
			log.Printf("[ERROR] failed to boot postgres in the background: %s", err)
		}
	}()

	conn, err := openConn(ctx, node.PrivateIP)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Drop repmgr database to clear any metadata that belonged to the old cluster.
	_, err = conn.Exec(ctx, "DROP DATABASE repmgr;")
	if err != nil {
		return fmt.Errorf("failed to drop repmgr database: %s", err)
	}

	// Create required users and ensure auth is configured to match the environment.
	if err := node.setupCredentials(ctx, conn); err != nil {
		return fmt.Errorf("failed creating required users: %s", err)
	}

	// Revert back to the original config file
	if err := restoreHBAFile(); err != nil {
		return fmt.Errorf("failed to restore original pg_hba.conf: %s", err)
	}

	svisor.Stop()

	// Wait for the postmaster to exit
	// TODO - This should be done in the supervisor
	if err := waitForPostmasterExit(ctx); err != nil {
		return fmt.Errorf("failed to wait for postmaster to exit: %s", err)
	}

	// Set the lock file so the init process knows not to restart
	// the restore process.
	if err := setRestoreLock(); err != nil {
		return fmt.Errorf("failed to set restore lock: %s", err)
	}

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	return nil
}

func waitForPostmasterExit(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	timeout := time.After(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			switch _, err := os.Stat(postmasterPath); {
			case os.IsNotExist(err):
				return nil
			case err != nil:
				return fmt.Errorf("error checking postmaster file: %v", err)
			}
		case <-timeout:
			return fmt.Errorf("timed out waiting for postmaster to exit")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func isRestoreActive() (bool, error) {
	if _, err := os.Stat(restoreLockFile); err == nil {
		val, err := os.ReadFile(restoreLockFile)
		if err != nil {
			return false, err
		}

		// TODO: This will cause problems if the backup originated
		// from an app with the same name.
		if string(val) == os.Getenv("FLY_APP_NAME") {
			return false, nil
		}
	}

	return true, nil
}

func backupHBAFile() error {
	if _, err := os.Stat(pathToHBAFile); err != nil {
		return err
	}

	val, err := os.ReadFile(pathToHBAFile)
	if err != nil {
		return err
	}

	return os.WriteFile(pathToHBABackup, val, 0600)
}

func grantLocalAccess() error {
	file, err := os.OpenFile(pathToHBAFile, os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	perm := []byte("host all postgres ::0/0 trust")
	_, err = file.Write(perm)
	if err != nil {
		return err
	}

	return file.Sync()
}

func restoreHBAFile() error {
	// open pg_hba backup
	data, err := os.ReadFile(pathToHBABackup)
	if err != nil {
		return err
	}

	// open the main pg_hba
	file, err := os.OpenFile(pathToHBAFile, os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// revert back to our original config
	if _, err = file.Write(data); err != nil {
		return err
	}

	// remove the backup
	if err := os.Remove(pathToHBABackup); err != nil {
		return err
	}

	return file.Sync()
}

func setRestoreLock() error {
	file, err := os.OpenFile(restoreLockFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	if _, err = file.WriteString(os.Getenv("FLY_APP_NAME")); err != nil {
		return err
	}

	return file.Sync()
}

func openConn(ctx context.Context, privateIP string) (*pgx.Conn, error) {
	url := fmt.Sprintf("postgres://[%s]:5433?target_session_attrs=any", privateIP)

	conf, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	conf.User = "postgres"

	// Allow up to 60 seconds for PG to boot and accept connections.
	timeout := time.After(60 * time.Second)
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-timeout:
			return nil, fmt.Errorf("timed out waiting for successful connection")
		case <-tick.C:
			conn, err := pgx.ConnectConfig(ctx, conf)
			if err == nil {
				return conn, err
			}

		}
	}
}

func clearLocks() error {
	if err := removeReadOnlyLock(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove readonly lock pre-restore: %s", err)
		}
	}

	if err := RemoveZombieLock(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove zombie lock pre-restore: %s", err)
		}
	}

	if err := removeRegistrationCert(); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove registration certificate: %s", err)
		}
	}

	return nil
}
