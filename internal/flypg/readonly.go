package flypg

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
)

const (
	readOnlyLockFile = "/data/locks/readonly.lock"

	readOnlyEnabled  = "on"
	readOnlyDisabled = "off"

	ReadOnlyStateEndpoint    = "commands/admin/readonly/state"
	BroadcastEnableEndpoint  = "commands/admin/readonly/enable"
	BroadcastDisableEndpoint = "commands/admin/readonly/disable"
)

func EnableReadonly(ctx context.Context, n *Node) error {
	if err := writeReadOnlyLock(); err != nil {
		return fmt.Errorf("failed to set readonly lock: %s", err)
	}

	if err := changeReadOnlyState(ctx, n, true); err != nil {
		return fmt.Errorf("failed to change readonly state to false: %s", err)
	}

	return nil
}

func DisableReadonly(ctx context.Context, n *Node) error {
	if !ReadOnlyLockExists() {
		return nil
	}

	if err := changeReadOnlyState(ctx, n, false); err != nil {
		return fmt.Errorf("failed to change readonly state to false: %s", err)
	}

	if err := removeReadOnlyLock(); err != nil {
		return fmt.Errorf("failed to remove readonly lock: %s", err)
	}

	return nil
}

// BroadcastReadonlyChange will communicate the readonly state change to all registered
// members.
func BroadcastReadonlyChange(ctx context.Context, n *Node, enabled bool) error {
	conn, err := n.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection: %s", err)
	}

	members, err := n.RepMgr.Members(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query standby members: %s", err)
	}

	target := BroadcastEnableEndpoint
	if !enabled {
		target = BroadcastDisableEndpoint
	}

	for _, member := range members {
		endpoint := fmt.Sprintf("http://[%s]:5500/%s", member.Hostname, target)
		resp, err := http.Get(endpoint)
		if err != nil {
			fmt.Printf("failed to broadcast readonly state to member %s: %s", member.Hostname, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode > 299 {
			fmt.Printf("failed to broadcast readonly state to member %s: %d\n", member.Hostname, resp.StatusCode)
		}
	}

	return nil
}

func ReadOnlyLockExists() bool {
	_, err := os.Stat(readOnlyLockFile)
	if os.IsNotExist(err) {
		return false
	}

	return true
}

func writeReadOnlyLock() error {
	if ReadOnlyLockExists() {
		return nil
	}

	if err := os.WriteFile(readOnlyLockFile, []byte(time.Now().String()), 0644); err != nil {
		return err
	}

	return nil
}

func removeReadOnlyLock() error {
	if !ReadOnlyLockExists() {
		return nil
	}

	if err := os.Remove(readOnlyLockFile); err != nil {
		return err
	}

	return nil
}

func changeReadOnlyState(ctx context.Context, n *Node, enable bool) error {
	conn, err := n.NewPrimaryConnection(ctx, "postgres")
	if err != nil {
		return fmt.Errorf("failed to establish connection: %s", err)
	}

	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to list database: %s", err)
	}

	var dbNames []string
	for _, db := range databases {
		// exclude administrative dbs
		if db.Name == "repmgr" || db.Name == "postgres" {
			continue
		}

		sql := fmt.Sprintf("ALTER DATABASE %s SET default_transaction_read_only=%v;", db.Name, enable)
		if _, err = conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to unset readonly on db %s: %s", db.Name, err)
		}

		dbNames = append(dbNames, db.Name)
	}

	bConn, err := n.PGBouncer.NewConnection(ctx)
	if err != err {
		return fmt.Errorf("failed to establish connection to pgbouncer: %s", err)
	}
	defer bConn.Close(ctx)

	poolMode, err := n.PGBouncer.poolMode()
	if err != nil {
		return fmt.Errorf("failed to resolve active pool mode: %s", err)
	}

	switch poolMode {
	case transactionPooler, statementPooler:
		if err := n.PGBouncer.forceReconnect(ctx, dbNames); err != nil {
			return fmt.Errorf("failed to force connection reset: %s", err)
		}
	case sessionPooler:
		if err := n.PGBouncer.killConnections(ctx, dbNames); err != nil {
			return fmt.Errorf("failed to kill connections: %s", err)
		}

		if err := n.PGBouncer.resumeConnections(ctx, dbNames); err != nil {
			return fmt.Errorf("failed to resume connections: %s", err)
		}
	default:
		return fmt.Errorf("failed to resolve valid pooler. found: %s", poolMode)
	}

	return nil
}
