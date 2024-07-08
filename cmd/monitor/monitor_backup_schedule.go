package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

const (
	backupRetryInterval = time.Second * 30
)

func monitorBackupSchedule(ctx context.Context, node *flypg.Node, barman *flypg.Barman) {
	lastBackupTime, err := barman.LastCompletedBackup(ctx)
	if err != nil {
		log.Printf("[WARN] Failed to resolve the last backup taken: %s", err)
	}

	// Calculate when the next backup is due.
	nextScheduledBackup := calculateNextBackupTime(barman, lastBackupTime)

	// Determine if the node is the primary.
	primary, err := isPrimary(ctx, node)
	if err != nil {
		log.Printf("[WARN] Failed to resolve primary status: %s", err)
	}

	if primary {
		if nextScheduledBackup < 0 {
			log.Println("[INFO] No backups found! Performing the initial base backup.")
			err := performBaseBackup(ctx, barman, true)
			switch {
			case err != nil:
				log.Printf("[WARN] Failed to perform initial base backup: %s", err)
				lastBackupTime = time.Now().Add(-backupFrequency(barman) + time.Hour)
			default:
				log.Println("[INFO] Initial base backup completed successfully")
				lastBackupTime = time.Now()
			}

			// Recalculate the next scheduled backup time after the initial backup.
			nextScheduledBackup = calculateNextBackupTime(barman, lastBackupTime)
		}

		log.Printf("[INFO] Next full backup due in: %s", nextScheduledBackup)
	}

	// Monitor the backup schedule even if we are not the primary. This is to ensure backups will
	// continue to be taken in the event of a failover.
	ticker := time.NewTicker(nextScheduledBackup)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[WARN] Shutting down backup schedule monitor...")
			return
		case <-ticker.C:
			// Check to see if we are the Primary.
			primary, err := isPrimary(ctx, node)
			if err != nil {
				log.Printf("[WARN] Failed to resolve primary status: %s", err)
			}

			// Noop if we are not the primary.
			if !primary {
				continue
			}

			lastBackupTime, err := barman.LastCompletedBackup(ctx)
			if err != nil {
				log.Printf("[WARN] Failed to determine when the last backup was taken: %s", err)
			}

			// Recalculate the next scheduled backup time.
			nextScheduledBackup = calculateNextBackupTime(barman, lastBackupTime)

			log.Printf("[INFO] Next full backup due in: %s", nextScheduledBackup)

			// Perform a full backup if the next scheduled backup time is less than 0.
			if nextScheduledBackup < 0 {
				log.Println("[INFO] Performing full backup now")
				err := performBaseBackup(ctx, barman, false)
				switch {
				case err != nil:
					log.Printf("[WARN] Failed to perform full backup: %s", err)
					// Retry the backup in 1 hour.
					nextScheduledBackup = time.Hour
				default:
					log.Println("[INFO] Full backup completed successfully")
					nextScheduledBackup = backupFrequency(barman)
				}
			}

			// Reset the ticker frequency in case the backup frequency has changed.
			ticker.Reset(nextScheduledBackup)
		}
	}
}

func calculateNextBackupTime(barman *flypg.Barman, lastBackupTime time.Time) time.Duration {
	// If there was no backup, return a negative duration to trigger an immediate backup.
	if lastBackupTime.IsZero() {
		return -1
	}
	return time.Until(lastBackupTime.Add(backupFrequency(barman)))
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
			log.Printf("[WARN] Failed to parse full backup frequency: %s", err)
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
			return ctx.Err()
		default:
			if _, err := barman.Backup(ctx, immediateCheckpoint); err != nil {
				log.Printf("[WARN] Failed to perform full backup: %s. Retrying in 30 seconds.", err)

				// If we've exceeded the maximum number of retries, we should return an error.
				if retryCount >= maxRetries {
					return fmt.Errorf("failed to perform full backup after %d retries", maxRetries)
				}

				retryCount++

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backupRetryInterval):
					continue
				}
			}

			return nil
		}
	}
}
