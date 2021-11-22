package flycheck

import (
	"context"

	chk "github.com/fly-apps/postgres-standalone/pkg/check"
)

// PostgreSQLRole outputs current role
func PostgreSQLRole(ctx context.Context, checks *chk.CheckSuite) (*chk.CheckSuite, error) {

	checks.AddCheck("role", func() (string, error) {
		return "leader", nil
	})
	return checks, nil
}
