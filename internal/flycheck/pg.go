package flycheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/superfly/fly-checks/check"
)

// Primary will be made read-only when disk capacity reaches this percentage.
const diskCapacityPercentageThreshold = 90.0

// CheckPostgreSQL health, replication, etc
func CheckPostgreSQL(ctx context.Context, checks *check.CheckSuite) (*check.CheckSuite, error) {
	node, err := flypg.NewNode()
	if err != nil {
		return checks, errors.Wrap(err, "failed to initialize node")
	}

	localConn, err := node.NewLocalConnection(ctx, "postgres")
	if err != nil {
		return checks, errors.Wrap(err, "failed to connect with local node")
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
		localConn.Close(ctx)
		repConn.Close(ctx)
	}

	checks.AddCheck("locks", func() (string, error) {
		locks := []string{}

		if flypg.ZombieLockExists() {
			locks = append(locks, "zombie")
		}
		if flypg.ReadOnlyLockExists() {
			locks = append(locks, "read-only")
		}

		if len(locks) > 0 {
			lockStr := strings.Join(locks, ", ")
			return "", fmt.Errorf(lockStr)
		}

		return "no locks detected", nil

	})

	checks.AddCheck("connections", func() (string, error) {
		return connectionCount(ctx, localConn)
	})

	return checks, nil
}

func connectionCount(ctx context.Context, local *pgx.Conn) (string, error) {
	sql := `select used, res_for_super as reserved, max_conn as max from
			(select count(*) used from pg_stat_activity) q1,
			(select setting::int res_for_super from pg_settings where name=$$superuser_reserved_connections$$) q2,
			(select setting::int max_conn from pg_settings where name=$$max_connections$$) q3`

	var used, reserved, max int

	err := local.QueryRow(ctx, sql).Scan(&used, &reserved, &max)

	if err != nil {
		return "", fmt.Errorf("%v", err)
	}

	return fmt.Sprintf("%d used, %d reserved, %d max", used, reserved, max), nil
}
