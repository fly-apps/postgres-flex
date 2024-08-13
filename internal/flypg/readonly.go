package flypg

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	readOnlyLockFile = "/data/readonly.lock"

	ReadOnlyStateEndpoint    = "commands/admin/readonly/state"
	BroadcastEnableEndpoint  = "commands/admin/readonly/enable"
	BroadcastDisableEndpoint = "commands/admin/readonly/disable"
	RestartHaproxyEndpoint   = "commands/admin/haproxy/restart"
)

func EnableReadonly(ctx context.Context, n *Node) error {
	if err := writeReadOnlyLock(); err != nil {
		return fmt.Errorf("failed to set read-only lock: %s", err)
	}

	if err := changeReadOnlyState(ctx, n, true); err != nil {
		return fmt.Errorf("failed to enable read-only mode: %s", err)
	}

	return nil
}

func DisableReadonly(ctx context.Context, n *Node) error {
	if !ReadOnlyLockExists() {
		return nil
	}

	if err := changeReadOnlyState(ctx, n, false); err != nil {
		return fmt.Errorf("failed to disable read-only mode: %s", err)
	}

	if err := removeReadOnlyLock(); err != nil {
		return fmt.Errorf("failed to remove read-only lock: %s", err)
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
	defer func() { _ = conn.Close(ctx) }()

	members, err := n.RepMgr.Members(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query standby members: %s", err)
	}

	target := BroadcastEnableEndpoint
	if !enabled {
		target = BroadcastDisableEndpoint
	}

	for _, member := range members {
		if member.Role == PrimaryRoleName {
			endpoint := fmt.Sprintf("http://%s:5500/%s", member.Hostname, target)
			resp, err := http.Get(endpoint)
			if err != nil {
				log.Printf("[WARN] Failed to broadcast readonly state change to member %s: %s", member.Hostname, err)
				continue
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode > 299 {
				log.Printf("[WARN] Failed to broadcast readonly state change to member %s: %d\n", member.Hostname, resp.StatusCode)
			}
		}
	}

	for _, member := range members {
		endpoint := fmt.Sprintf("http://%s:5500/%s", member.Hostname, RestartHaproxyEndpoint)
		resp, err := http.Get(endpoint)
		if err != nil {
			log.Printf("[WARN] Failed to restart haproxy on member %s: %s", member.Hostname, err)
			continue
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode > 299 {
			log.Printf("[WARN] Failed to restart haproxy on member %s: %d\n", member.Hostname, resp.StatusCode)
		}
	}

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	return nil
}

func ReadOnlyLockExists() bool {
	_, err := os.Stat(readOnlyLockFile)
	return !os.IsNotExist(err)
}

func writeReadOnlyLock() error {
	if ReadOnlyLockExists() {
		return nil
	}

	if err := os.WriteFile(readOnlyLockFile, []byte(time.Now().String()), 0600); err != nil {
		return fmt.Errorf("failed to create readonly.lock: %s", err)
	}

	if err := utils.SetFileOwnership(readOnlyLockFile, "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership: %s", err)
	}

	return nil
}

func removeReadOnlyLock() error {
	if !ReadOnlyLockExists() {
		return nil
	}

	return os.Remove(readOnlyLockFile)
}

func changeReadOnlyState(ctx context.Context, n *Node, enable bool) error {
	conn, err := n.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	member, err := n.RepMgr.Member(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to resolve member: %s", err)
	}

	// No need to enable read-only on standby's.
	if member.Role == PrimaryRoleName {
		databases, err := admin.ListDatabases(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to list databases: %s", err)
		}

		for _, db := range databases {
			// exclude administrative dbs
			if db.Name == "repmgr" || db.Name == "postgres" {
				continue
			}

			sql := fmt.Sprintf("ALTER DATABASE %q SET default_transaction_read_only=%v;", db.Name, enable)
			if _, err = conn.Exec(ctx, sql); err != nil {
				return fmt.Errorf("failed to alter readonly state on db %s: %s", db.Name, err)
			}
		}
	}

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	return nil
}
