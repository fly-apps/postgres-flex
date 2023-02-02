package flypg

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/jackc/pgx/v5"
)

const ReadOnlyLockFile = "/data/readonly.lock"

func SetReadOnly(ctx context.Context, n *Node, conn *pgx.Conn) error {
	if err := WriteReadOnlyLock(); err != nil {
		return fmt.Errorf("failed to set readonly lock: %s", err)
	}

	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		return err
	}

	for _, db := range databases {
		if db.Name == "repmgr" || db.Name == "postgres" {
			continue
		}

		dbConn, err := n.NewPrimaryConnection(ctx, db.Name)
		if err != err {
			return fmt.Errorf("failed to establish connection to db %s: %s", db.Name, err)
		}
		defer dbConn.Close(ctx)

		if _, err = dbConn.Exec(ctx, "SET default_transaction_read_only=true;"); err != nil {
			return fmt.Errorf("failed to set readonly on db %s: %s", db.Name, err)
		}

		var out string

		dbConn.QueryRow(ctx, "SHOW default_transaction_read_only;").Scan(&out)
		if err != nil {
			return fmt.Errorf("failed to verify readonly was unset: %s", err)
		}

		if out == "off" {
			return fmt.Errorf("failed to turn database '%s' readonly. value(%s)", db.Name, out)
		}

	}

	return nil
}

func UnsetReadOnly(ctx context.Context, n *Node, conn *pgx.Conn) error {
	// Skip if there's no readonly lock present
	if !ReadOnlyLockExists() {
		return nil
	}

	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		return err
	}

	for _, db := range databases {
		if db.Name == "repmgr" || db.Name == "postgres" {
			continue
		}

		dbConn, err := n.NewPrimaryConnection(ctx, db.Name)
		if err != err {
			return fmt.Errorf("failed to establish connection to db %s: %s", db.Name, err)
		}
		defer dbConn.Close(ctx)

		_, err = dbConn.Exec(ctx, "SET default_transaction_read_only=false;")
		if err != nil {
			return fmt.Errorf("failed to unset readonly on db %s: %s", db.Name, err)
		}

		// confirm

		var out string

		dbConn.QueryRow(ctx, "SHOW default_transaction_read_only;").Scan(&out)
		if err != nil {
			return fmt.Errorf("failed to verify readonly was unset: %s", err)
		}

		if out == "on" {
			return fmt.Errorf("failed to turn database '%s' read/write. value(%s): %s", db.Name, out, err)
		}

		if err := RemoveReadOnlyLock(); err != nil {
			return fmt.Errorf("failed to remove readonly lock file: %s", err)
		}
	}

	if err := RemoveReadOnlyLock(); err != nil {
		return fmt.Errorf("failed to remove readonly lock: %s", err)
	}

	return nil
}

func ReadOnlyLockExists() bool {
	_, err := os.Stat(ReadOnlyLockFile)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func WriteReadOnlyLock() error {
	if ReadOnlyLockExists() {
		return nil
	}

	if err := os.WriteFile(ReadOnlyLockFile, []byte(time.Now().String()), 0644); err != nil {
		return err
	}

	return nil
}

func RemoveReadOnlyLock() error {
	if !ReadOnlyLockExists() {
		return nil
	}

	if err := os.Remove(ReadOnlyLockFile); err != nil {
		return err
	}

	return nil
}
