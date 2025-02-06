package flypg

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"log"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	pathToPGControl = "/data/postgresql/global/pg_control"
)

func pgControlSettings(ctx context.Context) (map[string]string, error) {
	// Short-circuit if the pg_control file doesn't exist.
	if !utils.FileExists(pathToPGControl) {
		log.Println("[WARN] pg_control file does not exist. Skipping evaluation.")
		return nil, nil
	}

	result, err := utils.RunCmd(ctx, "root", "pg_controldata")
	if err != nil {
		return nil, fmt.Errorf("failed to run pg_controldata: %s", err)
	}

	return parsePGControlData(string(result))
}

func parsePGControlData(pgControlData string) (map[string]string, error) {
	settings := make(map[string]string)

	scanner := bufio.NewScanner(strings.NewReader(pgControlData))
	for scanner.Scan() {
		line := scanner.Text()

		// Filter out lines that don't contain the word "setting".
		if !strings.Contains(line, "setting:") {
			continue
		}

		parts := strings.SplitN(line, "setting:", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		settings[key] = value
	}

	// Check for any scanner errors.
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return settings, nil
}
