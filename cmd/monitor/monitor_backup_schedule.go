package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

func monitorBackupSchedule(ctx context.Context, barman *flypg.Barman) {
	// Determine when the last backup was taken.
	lastBackupTime, err := barman.LastBackupTaken(ctx)
	if err != nil {
		log.Printf("Failed to resolve the last backup taken: %s", err)
	}

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

	// Ensure we have a least one backup before proceeding.
	if lastBackupTime.IsZero() {
		log.Println("No backups found! Performing the initial base backup.")

		if err := performInitialBaseBackup(ctx, barman); err != nil {
			log.Printf("Failed to perform the initial full backup: %s", err)
			log.Printf("Backup scheduler will re-attempt in %s.", fullBackupSchedule)
		}

		lastBackupTime = time.Now()
	}

	log.Printf("Full backup schedule set to: %s", fullBackupSchedule)

	// Calculate the time until the next backup is due.
	timeUntilNextBackup := time.Until(lastBackupTime.Add(fullBackupSchedule))

	// Perform backup immediately if the time until the next backup is negative.
	if timeUntilNextBackup < 0 {
		log.Println("Performing full backup now")
		_, err := barman.Backup(ctx, false)
		if err != nil {
			log.Printf("Full backup failed with: %s", err)
		}

		timeUntilNextBackup = fullBackupSchedule
	}

	log.Printf("Next full backup due in: %s", timeUntilNextBackup)

	ticker := time.NewTicker(timeUntilNextBackup)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down backup scheduler")
			return
		case <-ticker.C:
			// Perform a backup while passively waiting for the checkpoint process to complete.
			// This could actually take a while, so we should be prepared to wait.
			log.Println("Performing full backup")
			_, err := barman.Backup(ctx, false)
			if err != nil {
				// TODO - Implement a backup-off strategy.
				timeUntilNextBackup = time.Hour * 1
				ticker.Reset(timeUntilNextBackup)

				log.Printf("Backup retention failed with: %s.", err)
				log.Printf("Backup will be re-attempted in %s.", timeUntilNextBackup)

				continue
			}

			log.Printf("Full backup completed successfully")
			ticker.Reset(fullBackupSchedule)
		}
	}
}

func performInitialBaseBackup(ctx context.Context, barman *flypg.Barman) error {
	maxRetries := 10
	retryCount := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err := barman.Backup(ctx, true)
			if err != nil {
				log.Printf("Failed to perform the initial full backup: %s. Retrying in 30 seconds.", err)

				// If we've exceeded the maximum number of retries, we should return an error.
				if retryCount >= maxRetries {
					return fmt.Errorf("failed to perform the initial full backup after %d retries", maxRetries)
				}

				retryCount++

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second * 30):
					continue
				}
			}

			log.Println("Initial full backup completed successfully")
			return nil
		}
	}
}
