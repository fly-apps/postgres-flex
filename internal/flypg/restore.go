package flypg

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	pathToHBAFile   = "/data/postgres/pg_hba.conf"
	pathToBackup    = "/data/postgres/pg_hba.conf.bak"
	restoreLockFile = "/data/restore.lock"
)

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

func Restore(ctx context.Context, node *Node) error {
	fmt.Println("Backing up HBA File")
	if err := backupHBAFile(); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed backing up pg_hba.conf: %s", err)
	}

	fmt.Println("Overwriting HBA file so we can update authentication")
	if err := overwriteHBAFile(); err != nil {
		return fmt.Errorf("failed to overwrite pg_hba.conf: %s", err)
	}

	if _, err := os.Stat("/data/postgres/standby.signal"); err == nil {
		fmt.Println("restoring from a hot standby. clearing standby signal so we can boot.")
		// We are restoring from a hot standby, so we need to clear the signal so we can boot.
		if err = os.Remove("/data/postgres/standby.signal"); err != nil {
			return fmt.Errorf("failed to remove standby signal: %s", err)
		}
	}

	fmt.Println("Starting PG in standalone mode")
	if err := utils.RunCommand(fmt.Sprintf("pg_ctl -D /data/postgres -h %s", node.PrivateIP)); err != nil {
		return fmt.Errorf("failed to start standalone postgres: %s", err)
	}

	fmt.Println("Establishing new connection to PG")
	conn, err := node.NewLocalConnection(ctx, "postgres")
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}

	fmt.Println("Establishing new connection to PG")
	if err = node.createRequiredUsers(ctx, conn); err != nil {
		return fmt.Errorf("failed creating required users: %s", err)
	}

	fmt.Println("Restore original HBA file")
	if err := restoreHBAFile(); err != nil {
		return fmt.Errorf("failed to restore original pg_hba.conf: %s", err)
	}

	if err := utils.RunCommand(fmt.Sprintf("pg_ctl -D /data/postgres -h %s", node.PrivateIP)); err != nil {
		return fmt.Errorf("failed to shutdown standalone postgres: %s", err)
	}

	if err := setRestoreLock(); err != nil {
		return fmt.Errorf("failed to set restore lock: %s", err)
	}

	return nil
}

func backupHBAFile() error {
	if _, err := os.Stat(pathToHBAFile); os.IsNotExist(err) {
		return err
	}

	input, err := ioutil.ReadFile(pathToHBAFile)
	if err != nil {
		return err
	}

	if err = ioutil.WriteFile(pathToBackup, input, 0644); err != nil {
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

	perm := []byte("host all flypgadmin ::0/0 trust")
	_, err = file.Write(perm)
	if err != nil {
		return err
	}

	return nil
}

func restoreHBAFile() error {
	input, err := ioutil.ReadFile(pathToBackup)
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

	if err := os.Remove(pathToBackup); err != nil {
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
