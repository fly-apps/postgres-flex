package flypg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

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
	b := &Barman{
		appName:  os.Getenv("FLY_APP_NAME"),
		provider: providerDefault,
		endpoint: strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL_S3")),
		bucket:   strings.TrimSpace(os.Getenv("AWS_BUCKET_NAME")),

		retentionDays:     getenv("CLOUD_ARCHIVING_RETENTION_DAYS", "7"),
		minimumRedundancy: getenv("CLOUD_ARCHIVING_MINIMUM_REDUNDANCY", "3"),
	}

	if validate {
		return b, b.ValidateRequiredEnv()
	}

	return b, nil
}

func (b *Barman) Backup(ctx context.Context) ([]byte, error) {
	backupCmd := fmt.Sprintf("barman-cloud-backup --cloud-provider %s --endpoint-url %s s3://%s %s",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)

	return utils.RunCommand(backupCmd, "postgres")
}

// RestoreBackup returns the command string used to restore a base backup.
func (b *Barman) RestoreBackup(ctx context.Context, backupID, recoveryDir string) ([]byte, error) {
	restoreCmd := fmt.Sprintf("barman-cloud-restore --cloud-provider %s --endpoint-url %s s3://%s %s %s %s",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
		backupID,
		recoveryDir,
	)

	return utils.RunCommand(restoreCmd, "postgres")
}

func (b *Barman) ListBackups(ctx context.Context) (BackupList, error) {
	listBackupCmd := fmt.Sprintf("barman-cloud-backup-list --cloud-provider %s --endpoint-url %s s3://%s %s --format json",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)

	backupsBytes, err := utils.RunCommand(listBackupCmd, "postgres")
	if err != nil {
		return BackupList{}, fmt.Errorf("failed to list backups: %s", err)
	}

	return b.parseBackups(backupsBytes)
}

func (b *Barman) WALArchiveDelete(ctx context.Context) ([]byte, error) {
	deleteCmd := fmt.Sprintf("barman-cloud-backup-delete --cloud-provider %s --endpoint-url %s --retention %s --minimum-redundancy %s s3://%s %s",
		b.provider,
		b.endpoint,
		b.RetentionPolicy(),
		b.minimumRedundancy,
		b.bucket,
		b.appName,
	)

	return utils.RunCommand(deleteCmd, "postgres")
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
	return fmt.Sprintf("barman-cloud-wal-archive --cloud-provider %s --gzip --endpoint-url %s s3://%s %s %%p",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)
}

// walRestoreCommand returns the command string used to restore WAL files.
// The %f and %p placeholders are replaced with the file path and file name respectively.
func (b *Barman) walRestoreCommand() string {
	return fmt.Sprintf("barman-cloud-wal-restore --cloud-provider %s --endpoint-url %s s3://%s %s %%f %%p",
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
