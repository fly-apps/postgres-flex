package flypg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	providerDefault = "aws-s3"
	// DefaultAuthProfile is the default AWS profile used for barman operations.
	DefaultAuthProfile = "barman"
	// RestoreAuthProfile is the AWS profile used for barman restore operations.
	RestoreAuthProfile = "restore"
)

type Barman struct {
	appName         string
	provider        string
	endpoint        string
	bucket          string
	bucketDirectory string
	authProfile     string
	store           *state.Store

	*BarmanConfig
}

type Backup struct {
	Status    string `json:"status"`
	BackupID  string `json:"backup_id"`
	StartTime string `json:"begin_time"`
	EndTime   string `json:"end_time"`
	BeginWal  string `json:"begin_wal"`
	EndWal    string `json:"end_wal"`
}

type BackupList struct {
	Backups []Backup `json:"backups_list"`
}

// NewBarman creates a new Barman instance.
// The configURL is expected to be in the format:
// https://s3-access-key:s3-secret-key@s3-endpoint/bucket/bucket-directory
func NewBarman(store *state.Store, configURL, authProfile string) (*Barman, error) {
	parsedURL, err := url.Parse(configURL)
	if err != nil {
		return nil, fmt.Errorf("invalid credential url: %w", err)
	}

	endpoint := parsedURL.Host
	if endpoint == "" {
		return nil, fmt.Errorf("object storage endpoint missing")
	}

	path := strings.TrimLeft(parsedURL.Path, "/")
	if path == "" {
		return nil, fmt.Errorf("bucket and directory missing")
	}

	pathSlice := strings.Split(path, "/")
	if len(pathSlice) != 2 {
		return nil, fmt.Errorf("invalid bucket and directory format")
	}

	// Extract user information for credentials (not used here but necessary for the complete parsing)
	username := parsedURL.User.Username()
	password, _ := parsedURL.User.Password()

	// Ensure the credentials are not empty
	if username == "" || password == "" {
		return nil, fmt.Errorf("access key or secret key is missing")
	}

	return &Barman{
		appName:         os.Getenv("FLY_APP_NAME"),
		provider:        providerDefault,
		endpoint:        fmt.Sprintf("https://%s", endpoint),
		bucket:          pathSlice[0],
		bucketDirectory: pathSlice[1],
		authProfile:     authProfile,
		store:           store,
	}, nil
}

func (b *Barman) LoadConfig(configDir string) error {
	barCfg, err := NewBarmanConfig(b.store, configDir)
	if err != nil {
		return err
	}

	b.BarmanConfig = barCfg

	return nil
}

func (b *Barman) BucketURL() string {
	return fmt.Sprintf("s3://%s", b.bucket)
}

// Backup performs a base backup of the database.
// immediateCheckpoint - forces the initial checkpoint to be done as quickly as possible.
func (b *Barman) Backup(ctx context.Context, immediateCheckpoint bool) ([]byte, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", b.endpoint,
		"--profile", b.authProfile,
		"--host", fmt.Sprintf("%s.internal", b.appName),
		"--user", "repmgr",
		b.BucketURL(),
		b.bucketDirectory,
	}

	if immediateCheckpoint {
		args = append(args, "--immediate-checkpoint")
	}

	return utils.RunCmd(ctx, "postgres", "barman-cloud-backup", args...)
}

// RestoreBackup returns the command string used to restore a base backup.
func (b *Barman) RestoreBackup(ctx context.Context, backupID string) ([]byte, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", b.endpoint,
		"--profile", b.authProfile,
		b.BucketURL(),
		b.bucketDirectory,
		backupID,
		defaultRestoreDir,
	}

	return utils.RunCmd(ctx, "postgres", "barman-cloud-restore", args...)
}

func (b *Barman) ListBackups(ctx context.Context) (BackupList, error) {
	args := []string{
		"--cloud-provider", providerDefault,
		"--endpoint-url", b.endpoint,
		"--profile", b.authProfile,
		"--format", "json",
		b.BucketURL(),
		b.bucketDirectory,
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
		"--endpoint-url", b.endpoint,
		"--profile", b.authProfile,
		"--retention", b.Settings.RecoveryWindow,
		"--minimum-redundancy", b.Settings.MinimumRedundancy,
		b.BucketURL(),
		b.bucketDirectory,
	}

	return utils.RunCmd(ctx, "postgres", "barman-cloud-backup-delete", args...)
}

func (b *Barman) ListCompletedBackups(ctx context.Context) (BackupList, error) {
	backups, err := b.ListBackups(ctx)
	if err != nil {
		return BackupList{}, fmt.Errorf("failed to list backups: %s", err)
	}

	var completedBackups BackupList

	for _, backup := range backups.Backups {
		if backup.Status == "DONE" {
			completedBackups.Backups = append(completedBackups.Backups, backup)
		}
	}

	return completedBackups, nil
}

func (b *Barman) LastCompletedBackup(ctx context.Context) (time.Time, error) {
	backups, err := b.ListCompletedBackups(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to list backups: %s", err)
	}

	if len(backups.Backups) == 0 {
		return time.Time{}, nil
	}

	// Layout used by barman.
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

func (b *Barman) walArchiveCommand() string {
	// TODO - Make compression configurable
	return fmt.Sprintf("barman-cloud-wal-archive --cloud-provider %s --gzip --endpoint-url %s --profile %s %s %s %%p",
		b.provider,
		b.endpoint,
		b.authProfile,
		b.BucketURL(),
		b.bucketDirectory,
	)
}

// walRestoreCommand returns the command string used to restore WAL files.
// The %f and %p placeholders are replaced with the file path and file name respectively.
func (b *Barman) walRestoreCommand() string {
	return fmt.Sprintf("barman-cloud-wal-restore --cloud-provider %s --endpoint-url %s --profile %s %s %s %%f %%p",
		b.provider,
		b.endpoint,
		b.authProfile,
		b.BucketURL(),
		b.bucketDirectory,
	)
}

func (*Barman) parseBackups(backupBytes []byte) (BackupList, error) {
	var backupList BackupList

	if err := json.Unmarshal(backupBytes, &backupList); err != nil {
		return BackupList{}, fmt.Errorf("failed to parse backups: %s", err)
	}

	return backupList, nil
}

func formatTimestamp(timestamp string) (string, error) {
	if strings.HasSuffix(timestamp, "Z") {
		timestamp = strings.TrimSuffix(timestamp, "Z") + "+00:00"
	}
	parsedTime, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return "", fmt.Errorf("failed to parse timestamp: %s", err)
	}

	return parsedTime.Format("2006-01-02T15:04:05-07:00"), nil
}
