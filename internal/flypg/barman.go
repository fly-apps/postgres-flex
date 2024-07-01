package flypg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

const (
	providerDefault    = "aws-s3"
	awsCredentialsPath = "/data/.aws/credentials"
)

type Barman struct {
	appName         string
	provider        string
	endpoint        string
	bucket          string
	bucketDirectory string

	// TODO - This was for convenience, but should probably be removed.
	configURL string

	retentionDays     string
	minimumRedundancy string
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

// NewBarman creates a new Barman instance.
// The configURL is expected to be in the format:
// https://s3-access-key:s3-secret-key@s3-endpoint/bucket/bucket-directory
func NewBarman(configURL string) (*Barman, error) {
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
		configURL:       configURL,

		retentionDays:     "7",
		minimumRedundancy: "3",
	}, nil

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
		"--retention", b.RetentionPolicy(),
		"--minimum-redundancy", b.minimumRedundancy,
		b.BucketURL(),
		b.bucketDirectory,
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
		b.BucketURL(),
		b.bucketDirectory,
	)
}

// walRestoreCommand returns the command string used to restore WAL files.
// The %f and %p placeholders are replaced with the file path and file name respectively.
func (b *Barman) walRestoreCommand() string {
	return fmt.Sprintf("barman-cloud-wal-restore --cloud-provider %s --endpoint-url %s %s %s %%f %%p",
		b.provider,
		b.endpoint,
		b.BucketURL(),
		b.bucketDirectory,
	)
}

func (b *Barman) writeAWSCredentials(profile string, credentialsPath string) error {
	barmanURL, err := url.Parse(b.configURL)
	if err != nil {
		return fmt.Errorf("invalid configURL: %w", err)
	}

	accessKey := barmanURL.User.Username()
	if accessKey == "" {
		return fmt.Errorf("AWS ACCESS KEY is missing")
	}

	secretAccessKey, _ := barmanURL.User.Password()
	if secretAccessKey == "" {
		return fmt.Errorf("AWS SECRET KEY is missing")
	}

	credentials := fmt.Sprintf("[%s]\naws_access_key_id=%s\naws_secret_access_key=%s",
		profile, accessKey, secretAccessKey)

	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(credentialsPath), 0700); err != nil {
		return fmt.Errorf("failed to create AWS credentials directory: %w", err)
	}

	if err := os.WriteFile(credentialsPath, []byte(credentials), 0644); err != nil {
		return fmt.Errorf("failed to write AWS credentials file: %w", err)
	}

	return nil
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}
