package flypg

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

type BarmanRestore struct {
	*Barman

	recoveryTargetName string
	recoveryTargetTime string
}

const (
	defaultRestoreDir = "/data/postgresql"
)

func NewBarmanRestore(configURL string) (*BarmanRestore, error) {
	barman, err := NewBarman(configURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create barman client: %s", err)
	}

	url, err := url.Parse(configURL)
	if err != nil {
		return nil, fmt.Errorf("invalid restore config url: %w", err)
	}

	restore := &BarmanRestore{
		Barman: barman,
	}

	// evaluate the query parameters
	for key, value := range url.Query() {
		switch key {
		case "targetName":
			restore.recoveryTargetName = value[0]
		case "targetTime":
			restore.recoveryTargetTime = value[0]
		default:
			return nil, fmt.Errorf("unknown query parameter: %s", key)
		}
	}

	if restore.recoveryTargetName == "" && restore.recoveryTargetTime == "" {
		return nil, fmt.Errorf("restore target not specified")
	}

	return restore, nil
}

func (b *BarmanRestore) Restore(ctx context.Context) error {
	// Query available backups from object storage
	backups, err := b.ListBackups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %s", err)
	}

	if len(backups.Backups) == 0 {
		return fmt.Errorf("no backups found")
	}

	var backupID string

	switch {
	case b.recoveryTargetName != "":
		// Resolve the target base backup

		// TODO - the id needs to be prefixed with barman_
		backupID, err = b.resolveBackupFromID(backups, b.recoveryTargetName)
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by id: %s", err)
		}
	case b.recoveryTargetTime != "":
		// Resolve the target base backup
		backupID, err = b.resolveBackupFromTime(backups, b.recoveryTargetTime)
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by time: %s", err)
		}
	default:
		return fmt.Errorf("restore target not specified")
	}

	// TODO - Consider just using the last available backup if the target is not found
	if backupID == "" {
		return fmt.Errorf("no backup found")
	}

	// Download and restore the base backup
	if _, err := b.RestoreBackup(ctx, backupID, defaultRestoreDir); err != nil {
		return fmt.Errorf("failed to restore backup: %s", err)
	}

	return nil
}

func (b *BarmanRestore) resolveBackupFromID(backupList BackupList, id string) (string, error) {
	if len(backupList.Backups) == 0 {
		return "", fmt.Errorf("no backups found")
	}

	for _, backup := range backupList.Backups {
		if backup.BackupID == id {
			return backup.BackupID, nil
		}
	}

	return "", fmt.Errorf("no backup found with id %s", id)
}

func (b *BarmanRestore) resolveBackupFromTime(backupList BackupList, restoreStr string) (string, error) {
	if len(backupList.Backups) == 0 {
		return "", fmt.Errorf("no backups found")
	}

	var restoreTime time.Time

	// Parse the restore string
	if restoreStr == "latest" {
		restoreTime = time.Now()
	} else {

		var err error
		restoreTime, err = time.Parse(time.RFC3339, restoreStr)
		if err != nil {
			return "", fmt.Errorf("failed to parse restore time: %s", err)
		}
	}

	latestBackupID := ""
	latestStartTime := time.Time{}

	earliestBackupID := ""
	earliestEndTime := time.Time{}

	layout := "Mon Jan 2 15:04:05 2006"

	for _, backup := range backupList.Backups {
		// Parse the backup start time
		startTime, err := time.Parse(layout, backup.StartTime)
		if err != nil {
			return "", fmt.Errorf("failed to parse backup start time: %s", err)
		}
		// Parse the backup start time
		endTime, err := time.Parse(layout, backup.EndTime)
		if err != nil {
			return "", fmt.Errorf("failed to parse backup end time: %s", err)
		}
		// Check if the restore time falls within the backup window
		if restoreTime.After(startTime) && restoreTime.Before(endTime) {
			return backup.BackupID, nil
		}

		// Track the latest and earliest backups in case the restore time is outside
		// the available backup windows
		if latestBackupID == "" || startTime.After(latestStartTime) {
			latestBackupID = backup.BackupID
			latestStartTime = startTime
		}

		if earliestBackupID == "" || endTime.Before(earliestEndTime) {
			earliestBackupID = backup.BackupID
			earliestEndTime = endTime
		}
	}

	// if the restore time is before the earliest backup, restore the earliest backup
	if restoreTime.Before(earliestEndTime) {
		return earliestBackupID, nil
	}

	return latestBackupID, nil
}
