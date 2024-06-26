package flypg

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fly-apps/postgres-flex/internal/utils"
)

type Barman struct {
	appName           string
	provider          string
	endpoint          string
	bucket            string
	minimumRedundancy string
	retentionDays     string
}

func NewBarman() (*Barman, error) {
	if err := validateBarmanRequirements(); err != nil {
		return nil, err
	}

	// TODO - Validate minimum and retention day values

	return &Barman{
		appName:           os.Getenv("FLY_APP_NAME"),
		provider:          "aws-s3",
		endpoint:          strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")),
		bucket:            strings.TrimSpace(os.Getenv("AWS_BUCKET_NAME")),
		minimumRedundancy: getenv("CLOUD_ARCHIVING_MINIMUM_REDUNDANCY", "3"),
		retentionDays:     getenv("CLOUD_ARCHIVING_RETENTION_DAYS", "7"),
	}, nil
}

func (b *Barman) RetentionPolicy() string {
	return fmt.Sprintf("'RECOVERY WINDOW OF %s days'", b.retentionDays)
}

// WALArchiveDelete deletes backups/WAL based on the specified retention policy.
func (b *Barman) WALArchiveDelete(ctx context.Context) ([]byte, error) {
	return utils.RunCommand(b.walArchiveDeleteCommandString(), "postgres")
}

func (b *Barman) PrintRetentionPolicy() {
	str := `
	Retention Policy
	-----------------
	RECOVERY WINDOW OF %s days
	MINIMUM BACKUP REDUNDANCY: %s
`
	log.Printf(str, b.retentionDays, b.minimumRedundancy)
}

func (b *Barman) walArchiveDeleteCommandString() string {
	return fmt.Sprintf("barman-cloud-backup-delete --cloud-provider %s --endpoint-url %s --retention %s --minimum-redundancy %s s3://%s %s",
		b.provider,
		b.endpoint,
		b.RetentionPolicy(),
		b.minimumRedundancy,
		b.bucket,
		b.appName,
	)
}

func (b *Barman) walArchiveCommandString() string {
	// TODO - Make compression configurable
	return fmt.Sprintf("barman-cloud-wal-archive --cloud-provider %s --gzip --endpoint-url %s s3://%s %s %%p",
		b.provider,
		b.endpoint,
		b.bucket,
		b.appName,
	)
}

func validateBarmanRequirements() error {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID secret must be set")
	}

	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return fmt.Errorf("AWS_SECRET_ACCESS_KEY secret must be set")
	}

	if os.Getenv("AWS_BUCKET_NAME") == "" {
		return fmt.Errorf("AWS_BUCKET_NAME envvar must be set")
	}

	if os.Getenv("AWS_ENDPOINT_URL") == "" {
		return fmt.Errorf("AWS_ENDPOINT_URL envvar must be set")
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
