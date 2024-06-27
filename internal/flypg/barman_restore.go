package flypg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

type BarmanRestore struct {
	appName  string
	provider string
	endpoint string
	bucket   string

	recoveryTarget         string
	recoveryTargetTimeline string
	recoveryTargetAction   string
}

type BarmanBackupList struct {
	Backups []BarmanBackup `json:"backups_list"`
}

type BarmanBackup struct {
	BackupID  string `json:"backup_id"`
	StartTime string `json:"begin_time"`
	EndTime   string `json:"end_time"`
	BeginWal  string `json:"begin_wal"`
	EndWal    string `json:"end_wal"`
}

func NewBarmanRestore() (*BarmanRestore, error) {
	if err := validateBarmanRestore(); err != nil {
		return nil, err
	}

	return &BarmanRestore{
		appName:  os.Getenv("FLY_APP_NAME"),
		provider: "aws-s3",
		endpoint: strings.TrimSpace(os.Getenv("SOURCE_AWS_ENDPOINT_URL")),
		bucket:   strings.TrimSpace(os.Getenv("SOURCE_AWS_BUCKET_NAME")),

		recoveryTarget: getenv("WAL_RECOVERY_TARGET", "immediate"),
		// TODO - Use recovery target time instead.  This is a temporary solution.
		recoveryTargetTimeline: getenv("WAL_RECOVERY_TARGET_TIMELINE", "latest"),
		recoveryTargetAction:   getenv("WAL_RECOVERY_TARGET_ACTION", "promote"),
	}, nil
}

func (b *BarmanRestore) RestoreFromPIT(ctx context.Context) error {
	// Query available backups from object storage
	backupsBytes, err := utils.RunCommand(b.backupListCommand(), "postgres")
	if err != nil {
		return fmt.Errorf("failed to list backups: %s", err)
	}

	// Parse the backups
	backupList, err := b.parseBackups(backupsBytes)
	if err != nil {
		return fmt.Errorf("failed to parse backups: %s", err)
	}

	if len(backupList.Backups) == 0 {
		return fmt.Errorf("no backups found")
	}

	// Resolve the base backup to restore
	backupID, err := b.resolveBackupTarget(backupList, b.recoveryTargetTimeline)
	if err != nil {
		return fmt.Errorf("failed to resolve backup target: %s", err)
	}

	// Download and restore the base backup
	_, err = utils.RunCommand(b.backupRestoreCommand(backupID), "postgres")
	if err != nil {
		return fmt.Errorf("failed to restore base backup: %s", err)
	}

	return nil
}

// restoreCommand returns the command string used to restore a base backup.
func (b *BarmanRestore) backupRestoreCommand(backupID string) string {
	return fmt.Sprintf("barman-cloud-restore --cloud-provider %s --endpoint-url %s s3://%s %s %s %s",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
		backupID,
		barmanRecoveryDirectory,
	)
}

// walRestoreCommand returns the command string used to restore WAL files.
// The %f and %p placeholders are replaced with the file path and file name respectively.
func (b *BarmanRestore) walRestoreCommand() string {
	return fmt.Sprintf("barman-cloud-wal-restore --cloud-provider %s --endpoint-url %s s3://%s %s %%f %%p",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)
}

func (b *BarmanRestore) backupListCommand() string {
	return fmt.Sprintf("barman-cloud-backup-list --cloud-provider %s --endpoint-url %s s3://%s %s --format json",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)
}

func (b *BarmanRestore) parseBackups(backupBytes []byte) (BarmanBackupList, error) {
	var backupList BarmanBackupList

	if err := json.Unmarshal(backupBytes, &backupList); err != nil {
		return BarmanBackupList{}, fmt.Errorf("failed to parse backups: %s", err)
	}

	return backupList, nil
}

func (b *BarmanRestore) resolveBackupTarget(backupList BarmanBackupList, restoreStr string) (string, error) {
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

func validateBarmanRestore() error {
	if os.Getenv("SOURCE_AWS_ACCESS_KEY_ID") == "" {
		return fmt.Errorf("SOURCE_AWS_ACCESS_KEY_ID secret must be set")
	}

	if os.Getenv("SOURCE_AWS_SECRET_ACCESS_KEY") == "" {
		return fmt.Errorf("SOURCE_AWS_SECRET_ACCESS_KEY secret must be set")
	}

	if os.Getenv("SOURCE_AWS_BUCKET_NAME") == "" {
		return fmt.Errorf("SOURCE_AWS_BUCKET_NAME envvar must be set")
	}

	if os.Getenv("SOURCE_AWS_ENDPOINT_URL") == "" {
		return fmt.Errorf("SOURCE_AWS_ENDPOINT_URL envvar must be set")
	}

	return nil
}
