package flycheck

import (
	"context"
	"fmt"
	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
	"github.com/superfly/fly-checks/check"
)

// Primary will be made read-only when disk capacity reaches this percentage.
const diskCapacityPercentageThreshold = 90.0

// CheckPostgreSQL health, replication, etc
func CheckPostgreSQL(ctx context.Context, checks *check.CheckSuite) (*check.CheckSuite, error) {
	node, err := flypg.NewNode()
	if err != nil {
		return checks, fmt.Errorf("failed to initialize node: %s", err)
	}

	localConn, err := node.NewLocalConnection(ctx, "postgres", node.SUCredentials)
	if err != nil {
		return checks, fmt.Errorf("failed to connect with local node: %s", err)
	}

	repConn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return checks, fmt.Errorf("failed to connect to repmgr node: %s", err)
	}

	member, err := node.RepMgr.Member(ctx, repConn)
	if err != nil {
		return checks, fmt.Errorf("failed to resolve local member role: %s", err)
	}

	// Cleanup connections
	checks.OnCompletion = func() {
		_ = localConn.Close(ctx)
		_ = repConn.Close(ctx)
	}

	checks.AddCheck("connections", func() (string, error) {
		return connectionCount(ctx, localConn)
	})

	checks.AddCheck("repmgr", func() (string, error) {
		return repmgrCheck()
	})

	if member.Role == flypg.PrimaryRoleName && member.Active {
		// Check that provides additional insight into disk capacity and
		// how close we are to hitting the readonly threshold.
		checks.AddCheck("disk-capacity", func() (string, error) {
			return diskCapacityCheck(ctx, node)
		})
	}

	return checks, nil
}

func repmgrCheck() (string, error) {
	cmd, err := utils.RunCommand("repmgr node check --config-file /data/repmgr.conf", "postgres")
	if err != nil {
		return string(cmd), err
	}
	return string(cmd), nil
}

func diskCapacityCheck(ctx context.Context, node *flypg.Node) (string, error) {
	// Calculate current disk usage
	size, available, err := diskUsage("/data/")
	if err != nil {
		return "", fmt.Errorf("failed to calculate disk usage: %s", err)
	}

	usedPercentage := float64(size-available) / float64(size) * 100

	// Turn primary read-only
	if usedPercentage > diskCapacityPercentageThreshold {
		// If the read-only lock has already been set, we can assume that we've already
		// broadcasted.
		if !flypg.ReadOnlyLockExists() {
			fmt.Println("Broadcasting readonly change to registered standbys")
			if err := flypg.BroadcastReadonlyChange(ctx, node, true); err != nil {
				fmt.Printf("errors with enable readonly broadcast: %s\n", err)
			}
		}

		return "", fmt.Errorf("%0.1f%% - readonly mode enabled, extend your volume to re-enable writes", usedPercentage)
	}

	// Don't attempt to disable readonly if there's a zombie.lock
	if !flypg.ZombieLockExists() && flypg.ReadOnlyLockExists() {
		if err := flypg.BroadcastReadonlyChange(ctx, node, false); err != nil {
			fmt.Printf("errors with disable readonly broadcast: %s\n", err)
		}
	}

	return fmt.Sprintf("%0.1f%% - readonly mode will be enabled at %0.1f%%", usedPercentage, diskCapacityPercentageThreshold), nil
}

func connectionCount(ctx context.Context, local *pgx.Conn) (string, error) {
	sql := `select used, res_for_super as reserved, max_conn as max from
			(select count(*) used from pg_stat_activity) q1,
			(select setting::int res_for_super from pg_settings where name=$$superuser_reserved_connections$$) q2,
			(select setting::int max_conn from pg_settings where name=$$max_connections$$) q3`

	var used, reserved, max int

	err := local.QueryRow(ctx, sql).Scan(&used, &reserved, &max)
	if err != nil {
		return "", fmt.Errorf("failed to query connection count: %s", err)
	}

	return fmt.Sprintf("%d used, %d reserved, %d max", used, reserved, max), nil
}
