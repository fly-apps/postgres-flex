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
	fmt.Println("Backing up HBA File")
	if err := backupHBAFile(); err != nil {
		fmt.Println(err)
		if os.IsNotExist(err) {
			fmt.Println("HBA FILE is not here for some reason, sleep for investigation")
			time.Sleep(2 * time.Minute)
			return err
		}
		return fmt.Errorf("failed backing up pg_hba.conf: %s", err)
	}

	fmt.Println("Overwriting HBA file so we can update authentication")
	if err := overwriteHBAFile(); err != nil {
		return fmt.Errorf("failed to overwrite pg_hba.conf: %s", err)
	}

	if _, err := os.Stat("/data/postgresql/standby.signal"); err == nil {
		fmt.Println("restoring from a hot standby. clearing standby signal so we can boot.")
		// We are restoring from a hot standby, so we need to clear the signal so we can boot.
		if err = os.Remove("/data/postgresql/standby.signal"); err != nil {
			return fmt.Errorf("failed to remove standby signal: %s", err)
		}
	}

	fmt.Println("Starting PG in standalone mode")

	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("postgres", fmt.Sprintf("gosu postgres postgres -D /data/postgresql -p 5433 -h %s", node.PrivateIP))
	go svisor.Run()

	fmt.Println("Establishing new connection to PG")
	conn, err := openConn(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}

	// Blow away repmgr database
	fmt.Println("Dropping repmgr database")
	sql := fmt.Sprintf("DROP DATABASE repmgr;")
	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop repmgr database: %s", err)
	}

	fmt.Println("Establishing new connection to PG")
	if err = node.createRequiredUsers(ctx, conn); err != nil {
		return fmt.Errorf("failed creating required users: %s", err)
	}

	fmt.Println("Restore original HBA file")
	if err := restoreHBAFile(); err != nil {
		return fmt.Errorf("failed to restore original pg_hba.conf: %s", err)
	}

	svisor.Stop()

	if err := setRestoreLock(); err != nil {
		return fmt.Errorf("failed to set restore lock: %s", err)
	}

	return nil
}

func isActiveRestore() (bool, error) {
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

func overwriteHBAFile() error {
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
	input, err := os.ReadFile(pathToHBABackup)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(pathToHBAFile, os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(input)
	if err != nil {
		return err
	}

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
	timeout := time.After(2 * time.Minute)
	tick := time.Tick(1 * time.Second)
	for {
		select {
		case <-timeout:
			return nil, fmt.Errorf("timed out waiting for successful connection")
		case <-tick:
			conn, err := pgx.ConnectConfig(ctx, conf)
			if err == nil {
				return conn, err
			}
		}
	}
}
