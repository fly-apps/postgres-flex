package flypg

import (
	"context"
	"fmt"
	"os"
	"time"
)

type BarmanRestore struct {
	*Barman
	recoveryTarget         string
	recoveryTargetTimeline string
	recoveryTargetAction   string
}

const (
	defaultRestoreDir = "/data/postgresql"
)

func NewBarmanRestore() (*BarmanRestore, error) {
	if err := validateRestoreEnv(); err != nil {
		return nil, err
	}

	barman, _ := NewBarman(false)

	return &BarmanRestore{
		Barman: barman,

		recoveryTarget: getenv("WAL_RECOVERY_TARGET", "immediate"),
		// TODO - Use recovery target time instead.  This is a temporary solution.
		recoveryTargetTimeline: getenv("WAL_RECOVERY_TARGET_TIMELINE", "latest"),
		recoveryTargetAction:   getenv("WAL_RECOVERY_TARGET_ACTION", "promote"),
	}, nil
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

	// Resolve the base backup to restore
	backupID, err := b.resolveBackupTarget(backups, b.recoveryTargetTimeline)
	if err != nil {
		return fmt.Errorf("failed to resolve backup target: %s", err)
	}

	// Download and restore the base backup
	if _, err := b.RestoreBackup(ctx, backupID, defaultRestoreDir); err != nil {
		return fmt.Errorf("failed to restore backup: %s", err)
	}

	return nil
}

func (b *BarmanRestore) resolveBackupTarget(backupList BackupList, restoreStr string) (string, error) {
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

func validateRestoreEnv() error {
	if os.Getenv("SOURCE_AWS_ACCESS_KEY_ID") == "" {
		return fmt.Errorf("SOURCE_AWS_ACCESS_KEY_ID secret must be set")
	}

	if os.Getenv("SOURCE_AWS_SECRET_ACCESS_KEY") == "" {
		return fmt.Errorf("SOURCE_AWS_SECRET_ACCESS_KEY secret must be set")
	}

	if os.Getenv("SOURCE_AWS_BUCKET_NAME") == "" {
		return fmt.Errorf("SOURCE_AWS_BUCKET_NAME envvar must be set")
	}

	if os.Getenv("SOURCE_AWS_ENDPOINT_URL_S3") == "" {
		return fmt.Errorf("SOURCE_AWS_ENDPOINT_URL_S3 envvar must be set")
	}

	return nil
}
