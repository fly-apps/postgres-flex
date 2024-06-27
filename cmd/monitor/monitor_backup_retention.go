package main

import (
	"context"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

const (
	defaultBackupRetentionEvaluationThreshold = time.Hour * 6
)

func monitorBackupRetention(ctx context.Context, barman *flypg.Barman) {
	ticker := time.NewTicker(defaultBackupRetentionEvaluationThreshold)
	defer ticker.Stop()
	for range ticker.C {
		result, err := barman.WALArchiveDelete(ctx)
		if err != nil {
			log.Printf("Backup retention failed with: %s", err)
		}

		if len(result) > 0 {
			log.Printf("Backup retention response: %s", result)
		}
	}
}
