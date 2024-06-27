package flypg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	providerDefault = "aws-s3"
)

type Barman struct {
	appName  string
	provider string
	endpoint string
	bucket   string

	fullBackupFrequency string // TODO - Implement
	minimumRedundancy   string
	retentionDays       string
}

type Backup struct {
	BackupID  string `json:"backup_id"`
	StartTime string `json:"begin_time"`
	EndTime   string `json:"end_time"`
	BeginWal  string `json:"begin_wal"`
	EndWal    string `json:"end_wal"`
}

type BackupList struct {
	Backups []Backup `json:"backups_list"`
}

func NewBarman(validate bool) (*Barman, error) {
	bucket := strings.TrimSpace(os.Getenv("AWS_BUCKET_NAME"))
	bucket = fmt.Sprintf("s3://%s", bucket)

	b := &Barman{
		appName:  os.Getenv("FLY_APP_NAME"),
		provider: providerDefault,
		endpoint: strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL_S3")),
		bucket:   bucket,

		retentionDays:     getenv("CLOUD_ARCHIVING_RETENTION_DAYS", "7"),
		minimumRedundancy: getenv("CLOUD_ARCHIVING_MINIMUM_REDUNDANCY", "3"),
	}

	if validate {
		return b, b.ValidateRequiredEnv()
	}

	return b, nil
}

// Backup performs a base backup of the database.
// forceCheckpoint - forces the initial checkpoint to be done as quickly as possible.
func (b *Barman) Backup(ctx context.Context, forceCheckpoint bool) ([]byte, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", os.Getenv("AWS_ENDPOINT_URL_S3"),
		"--host", fmt.Sprintf("%s.internal", b.appName),
		"--user", "repmgr",
		b.bucket,
		b.appName,
	}

	return utils.RunCmd(ctx, "postgres", "barman-cloud-backup", args...)
}

// RestoreBackup returns the command string used to restore a base backup.
func (b *Barman) RestoreBackup(ctx context.Context, backupID, recoveryDir string) ([]byte, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", os.Getenv("AWS_ENDPOINT_URL_S3"),
		b.bucket,
		b.appName,
		backupID,
		recoveryDir,
	}

	return utils.RunCmd(ctx, "postgres", "barman-cloud-restore", args...)
}

func (b *Barman) ListBackups(ctx context.Context) (BackupList, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", os.Getenv("AWS_ENDPOINT_URL_S3"),
		"--format", "json",
		b.bucket,
		b.appName,
	}

	backupsBytes, err := utils.RunCmd(ctx, "postgres", "barman-cloud-backup-list", args...)
	if err != nil {
		return BackupList{}, fmt.Errorf("failed to list backups: %s", err)
	}

	return b.parseBackups(backupsBytes)
}

func (b *Barman) WALArchiveDelete(ctx context.Context) ([]byte, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", os.Getenv("AWS_ENDPOINT_URL_S3"),
		"--retention", b.RetentionPolicy(),
		"--minimum-redundancy", b.minimumRedundancy,
		b.bucket,
		b.appName,
	}

	return utils.RunCmd(ctx, "postgres", "barman-cloud-backup-delete", args...)
}

func (b *Barman) LastBackupTaken(ctx context.Context) (time.Time, error) {
	backups, err := b.ListBackups(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to list backups: %s", err)
	}

	if len(backups.Backups) == 0 {
		return time.Time{}, nil
	}

	layout := "Mon Jan 2 15:04:05 2006"

	var latestBackupTime time.Time

	// Sort the backups start time
	for _, backup := range backups.Backups {
		startTime, err := time.Parse(layout, backup.StartTime)
		if err != nil {
			return time.Time{}, fmt.Errorf("failed to parse backup start time: %s", err)
		}

		if latestBackupTime.IsZero() || startTime.After(latestBackupTime) {
			latestBackupTime = startTime
		}
	}

	return latestBackupTime, nil
}

func (b *Barman) PrintRetentionPolicy() {
	log.Printf(`
Retention Policy
-----------------
RECOVERY WINDOW:  %s DAYS
MINIMUM BACKUP REDUNDANCY: %s`, b.retentionDays, b.minimumRedundancy)
}

func (b *Barman) RetentionPolicy() string {
	return fmt.Sprintf("'RECOVERY WINDOW OF %s days'", b.retentionDays)
}

func (b *Barman) ValidateRequiredEnv() error {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID secret must be set")
	}

	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return fmt.Errorf("AWS_SECRET_ACCESS_KEY secret must be set")
	}

	if os.Getenv("AWS_BUCKET_NAME") == "" {
		return fmt.Errorf("AWS_BUCKET_NAME envvar must be set")
	}

	if os.Getenv("AWS_ENDPOINT_URL_S3") == "" {
		return fmt.Errorf("AWS_ENDPOINT_URL_S3 envvar must be set")
	}

	return nil
}

func (b *Barman) parseBackups(backupBytes []byte) (BackupList, error) {
	var backupList BackupList

	if err := json.Unmarshal(backupBytes, &backupList); err != nil {
		return BackupList{}, fmt.Errorf("failed to parse backups: %s", err)
	}

	return backupList, nil
}

func (b *Barman) walArchiveCommand() string {
	// TODO - Make compression configurable
	return fmt.Sprintf("barman-cloud-wal-archive --cloud-provider %s --gzip --endpoint-url %s %s %s %%p",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)
}

// walRestoreCommand returns the command string used to restore WAL files.
// The %f and %p placeholders are replaced with the file path and file name respectively.
func (b *Barman) walRestoreCommand() string {
	return fmt.Sprintf("barman-cloud-wal-restore --cloud-provider %s --endpoint-url %s %s %s %%f %%p",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
