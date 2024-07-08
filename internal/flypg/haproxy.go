package flypg

import (
	"context"
	"fmt"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

func RestartHaproxy(ctx context.Context) error {
	if _, err := utils.RunCmd(ctx, "root", "restart-haproxy"); err != nil {
		return fmt.Errorf("failed to restart haproxy: %s", err)
	}

	return nil
}
