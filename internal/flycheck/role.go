package flycheck

import (
	"context"
	"fmt"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/superfly/fly-checks/check"
)

// PostgreSQLRole outputs current role
func PostgreSQLRole(ctx context.Context, checks *check.CheckSuite) (*check.CheckSuite, error) {
	node, err := flypg.NewNode()
	if err != nil {
		return checks, fmt.Errorf("failed to initialize node: %s", err)
	}

	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return checks, fmt.Errorf("failed to connect to local node: %s", err)
	}

	// Cleanup connections
	checks.OnCompletion = func() {
		conn.Close(ctx)
	}

	checks.AddCheck("role", func() (string, error) {
		member, err := node.RepMgr.Member(ctx, conn)
		if err != nil {
			return "", fmt.Errorf("failed to check role: %s", err)
		}

		switch member.Role {
		case flypg.PrimaryRoleName:
			return flypg.PrimaryRoleName, nil
		case flypg.StandbyRoleName:
			return flypg.StandbyRoleName, nil
		default:
			return flypg.UnknownRoleName, nil
		}
	})
	return checks, nil
}
