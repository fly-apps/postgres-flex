package main

import (
	"context"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func monitorBackupRetention(ctx context.Context, node *flypg.Node, barman *flypg.Barman) {
	ticker := time.NewTicker(defaultBackupRetentionEvalFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down backup retention monitor")
			return
		case <-ticker.C:
			primary, err := isPrimary(ctx, node)
			if err != nil {
				log.Printf("Failed to resolve primary when evaluating retention: %s", err)
				continue
			}

			if !primary {
				continue
			}

			if _, err := barman.WALArchiveDelete(ctx); err != nil {
				log.Printf("WAL archive retention failed with: %s", err)
			}

		}
	}
}
