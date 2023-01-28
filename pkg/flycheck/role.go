package flycheck

import (
	"context"
	"fmt"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/pkg/errors"
	"github.com/superfly/fly-checks/check"
)

// PostgreSQLRole outputs current role
func PostgreSQLRole(ctx context.Context, checks *check.CheckSuite) (*check.CheckSuite, error) {
	node, err := flypg.NewNode()
	if err != nil {
		return checks, errors.Wrap(err, "failed to initialize node")
	}

	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return checks, errors.Wrap(err, "failed to connect to local node")
	}

	// Cleanup connections
	checks.OnCompletion = func() {
		conn.Close(ctx)
	}

	checks.AddCheck("role", func() (string, error) {
		if flypg.ZombieLockExists() {
			return "zombie", fmt.Errorf("member is in a zombie state. see logs for more details")
		}

		member, err := node.RepMgr.Member(ctx, conn)
		if err != nil {
			return "failed", errors.Wrap(err, "failed to check role")
		}

		switch member.Role {
		case flypg.PrimaryRoleName:
			return "primary", nil
		case flypg.StandbyRoleName:
			return "replica", nil
		default:
			return "unknown", nil
		}
	})
	return checks, nil
}
