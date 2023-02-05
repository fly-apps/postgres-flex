package flypg

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/supervisor"
	"github.com/jackc/pgx/v5"
)

const (
	pathToHBAFile   = "/data/postgresql/pg_hba.conf"
	pathToHBABackup = "/data/postgresql/pg_hba.conf.bak"
	restoreLockFile = "/data/restore.lock"
)

func Restore(ctx context.Context, node *Node) error {
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
		fmt.Println("Restoring from a hot standby.")
		// Clear the signal so we can boot.
		if err = os.Remove("/data/postgresql/standby.signal"); err != nil {
			return fmt.Errorf("failed to remove standby signal: %s", err)
		}
	}

	// Boot postgres in standalone mode
	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("postgres", fmt.Sprintf("gosu postgres postgres -D /data/postgresql -p 5433 -h %s", node.PrivateIP))

	go svisor.Run()

	conn, err := openConn(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}

	// Drop repmgr database to clear any metadata that belonged to the old cluster.
	sql := "DROP DATABASE repmgr;"
	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop repmgr database: %s", err)
	}

	// Create required users and ensure auth is configured to match the environment.
	if err = node.createRequiredUsers(ctx, conn); err != nil {
		return fmt.Errorf("failed creating required users: %s", err)
	}

	// Revert back to the original config file
	if err := restoreHBAFile(); err != nil {
		return fmt.Errorf("failed to restore original pg_hba.conf: %s", err)
	}

	svisor.Stop()

	// Set the lock file so the init process knows not to restart
	// the restore process.
	if err := setRestoreLock(); err != nil {
		return fmt.Errorf("failed to set restore lock: %s", err)
	}

	return nil
}

func isRestoreActive() (bool, error) {
	if _, err := os.Stat(restoreLockFile); err == nil {
		val, err := os.ReadFile(restoreLockFile)
		if err != nil {
			return false, err
		}

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

	if err = ioutil.WriteFile(pathToHBABackup, val, 0644); err != nil {
		return err
	}

	return nil
}

func grantLocalAccess() error {
	file, err := os.OpenFile(pathToHBAFile, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	perm := []byte("host all postgres ::0/0 trust")
	_, err = file.Write(perm)
	if err != nil {
		return err
	}

	return nil
}

func restoreHBAFile() error {
	// open pg_hba backup
	data, err := os.ReadFile(pathToHBABackup)
	if err != nil {
		return err
	}

	// open the main pg_hba
	file, err := os.OpenFile(pathToHBAFile, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// revert back to our original config
	_, err = file.Write(data)
	if err != nil {
		return err
	}

	// remove the backup
	if err := os.Remove(pathToHBABackup); err != nil {
		return err
	}

	return nil
}

func setRestoreLock() error {
	file, err := os.OpenFile(restoreLockFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(os.Getenv("FLY_APP_NAME"))
	if err != nil {
		return err
	}

	return nil
}

func openConn(ctx context.Context, n *Node) (*pgx.Conn, error) {
	mode := "any"

	url := fmt.Sprintf("postgres://[%s]:5433?target_session_attrs=%s", n.PrivateIP, mode)
	conf, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	conf.User = "postgres"

	// Allow up to 30 seconds for PG to boot and accept connections.
	timeout := time.After(30 * time.Second)
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

	return nil
}
