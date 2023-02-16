package flypg

import (
	"fmt"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

func RestartHaproxy() error {
	if err := utils.RunCommand("restart-haproxy", "root"); err != nil {
		return fmt.Errorf("failed to restart haproxy: %s", err)
	}

	return nil
}
