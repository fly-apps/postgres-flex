package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func monitorBackupSchedule(ctx context.Context, node *flypg.Node, barman *flypg.Barman) {
	nextScheduledBackup, err := calculateNextBackupTime(ctx, barman)
	if err != nil {
		log.Printf("Failed to calculate the next scheduled backup time: %s", err)
	}

	primary, err := isPrimary(ctx, node)
	if err != nil {
		log.Printf("Failed to resolve primary status: %s", err)
	}

	if primary {

		if nextScheduledBackup < 0 {
			log.Println("No backups found! Performing the initial base backup.")
			if err := performBaseBackup(ctx, barman, true); err != nil {
				log.Printf("Failed to perform full backup: %s", err)
			}

			// Recalculate the next scheduled backup time after the initial backup.
			nextScheduledBackup, err = calculateNextBackupTime(ctx, barman)
			if err != nil {
				log.Printf("Failed to calculate the next scheduled backup time: %s", err)
			}
		}

		log.Printf("Next full backup due in: %s", nextScheduledBackup)
	}

	ticker := time.NewTicker(nextScheduledBackup)
	defer ticker.Stop()

	for range ticker.C {
		primary, err := isPrimary(ctx, node)
		if err != nil {
			log.Printf("Failed to resolve primary status: %s", err)
		}

		// Short-circuit if the node is not the primary.
		if !primary {
			continue
		}

		// Calculate when the next backup is due. This needs to be calculated per-tick
		// in case the backup frequency has changed or the primary status has changed.
		nextScheduledBackup, err := calculateNextBackupTime(ctx, barman)
		if err != nil {
			log.Printf("Failed to calculate the next scheduled backup time: %s", err)
		}

		if err := monitorBackupScheduleTick(ctx, node, barman); err != nil {
			log.Printf("monitorBackupScheduleTick failed with: %s", err)
		}

		// Reset the ticker frequency in case the backup frequency has changed.
		ticker.Reset(backupFrequency(barman))
	}
}

// func performInitialBackup(ctx context.Context, node *flypg.Node, barman *flypg.Barman) error {
// 	primary, err := isPrimary(ctx, node)
// 	if err != nil {
// 		log.Printf("Failed to resolve primary status: %s", err)
// 	}

// 	// Short-circuit if the node is not the primary.
// 	if !primary {
// 		return nil
// 	}

// 	// Determine when the last backup was taken.
// 	lastBackupTime, err := barman.LastCompletedBackup(ctx)
// 	if err != nil {
// 		log.Printf("Failed to resolve the last backup taken: %s", err)
// 	}

// 	// Perform the initial backup if the node is the primary.
// 	if lastBackupTime.IsZero() {
// 		log.Println("No backups found! Performing the initial base backup.")

// 		if err := performBaseBackup(ctx, barman); err != nil {
// 			log.Printf("Failed to perform the initial full backup: %s", err)
// 			log.Printf("Backup scheduler will re-attempt in %s.", backupFrequency(barman))
// 		}
// 	}

// 	return nil
// }

func monitorBackupScheduleTick(ctx context.Context, node *flypg.Node, barman *flypg.Barman) error {
	primary, err := isPrimary(ctx, node)
	if err != nil {
		return fmt.Errorf("failed to resolve primary status: %s", err)
	}

	// Short-circuit if the node is not the primary.
	if !primary {
		return nil
	}

	timeUntilNextBackup, err := calculateNextBackupTime(ctx, barman)
	if err != nil {
		return fmt.Errorf("failed to calculate the next scheduled backup time: %s", err)
	}

	// Perform backup immediately if the time until the next backup is negative.
	if timeUntilNextBackup < 0 {
		log.Println("Performing full backup now")
		if err := performBaseBackup(ctx, barman); err != nil {
			return fmt.Errorf("failed to perform full backup: %s", err)
		}
	}

	log.Printf("Next full backup due in: %s", timeUntilNextBackup)

}

func calculateNextBackupTime(ctx context.Context, barman *flypg.Barman) (time.Duration, error) {
	frequency := backupFrequency(barman)

	lastBackupTime, err := barman.LastCompletedBackup(ctx)
	if err != nil {
		log.Printf("Failed to resolve the last backup taken: %s", err)
	}

	if lastBackupTime.IsZero() {
		lastBackupTime = time.Now()
	}

	// Calculate the time until the next backup is due.
	return time.Until(lastBackupTime.Add(frequency)), nil
}

func isPrimary(ctx context.Context, node *flypg.Node) (bool, error) {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to open local connection: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	return node.RepMgr.IsPrimary(ctx, conn)
}

func backupFrequency(barman *flypg.Barman) time.Duration {
	fullBackupSchedule := defaultFullBackupSchedule

	// Set the full backup schedule if it is defined in the configuration.
	if barman.Settings.FullBackupFrequency != "" {
		fullBackupDur, err := time.ParseDuration(barman.Settings.FullBackupFrequency)
		switch {
		case err != nil:
			log.Printf("Failed to parse full backup frequency: %s", err)
		default:
			fullBackupSchedule = fullBackupDur
		}
	}

	return fullBackupSchedule
}

func performBaseBackup(ctx context.Context, barman *flypg.Barman, immediateCheckpoint bool) error {
	maxRetries := 10
	retryCount := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err := barman.Backup(ctx, immediateCheckpoint)
			if err != nil {
				log.Printf("failed to perform full backup: %s. Retrying in 30 seconds.", err)

				// If we've exceeded the maximum number of retries, we should return an error.
				if retryCount >= maxRetries {
					return fmt.Errorf("failed to perform full backup after %d retries", maxRetries)
				}

				retryCount++

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 30):
					continue
				}
			}

			log.Println("full backup completed successfully")
			return nil
		}
	}
}
