package main

import (
	"context"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func monitorBackupRetention(ctx context.Context, barman *flypg.Barman) {
	ticker := time.NewTicker(defaultBackupRetentionEvalFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down backup retention monitor")
			return
		case <-ticker.C:
			result, err := barman.WALArchiveDelete(ctx)
			if err != nil {
				log.Printf("Backup retention failed with: %s", err)
			}

			if len(result) > 0 {
				log.Printf("Backup retention response: %s", result)
			}
		}
	}
}
