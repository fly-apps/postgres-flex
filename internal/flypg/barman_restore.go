package flypg

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/supervisor"
)

type BarmanRestore struct {
	*Barman

	recoveryTarget          string
	recoveryTargetName      string
	recoveryTargetTime      string
	recoveryTargetAction    string
	recoveryTargetInclusive bool
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
		case "target":
			restore.recoveryTarget = value[0]
		case "targetName":
			restore.recoveryTargetName = value[0]
		case "targetTime":
			restore.recoveryTargetTime = value[0]
		case "targetInclusive":
			restore.recoveryTargetInclusive = value[0] == "true"
		case "targetAction":
			restore.recoveryTargetAction = value[0]
		default:
			return nil, fmt.Errorf("unknown query parameter: %s", key)
		}
	}

	if restore.recoveryTargetAction == "" {
		restore.recoveryTargetAction = "promote"
	}

	if restore.recoveryTargetName == "" && restore.recoveryTargetTime == "" && restore.recoveryTarget == "" {
		return nil, fmt.Errorf("no restore target not specified")
	}

	return restore, nil
}

func (b *BarmanRestore) WALReplayAndReset(ctx context.Context, node *Node) error {
	// create a copy of the pg_hba.conf file so we can revert back to it when needed.
	if err := backupHBAFile(); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed backing up pg_hba.conf: %s", err)
	}

	// Grant local access so we can update internal credentials
	// to match the environment.
	if err := grantLocalAccess(); err != nil {
		return fmt.Errorf("failed to grant local access: %s", err)
	}

	// Boot postgres and wait for WAL replay to complete
	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("postgres", fmt.Sprintf("gosu postgres postgres -D /data/postgresql -p 5433 -h %s", node.PrivateIP))

	// Start the postgres process in the background.
	go func() {
		if err := svisor.Run(); err != nil {
			log.Printf("[ERROR] failed to boot postgres in the background: %s", err)
		}
	}()

	// Wait for the WAL replay to complete.
	if err := b.waitOnRecovery(ctx, node.PrivateIP); err != nil {
		return fmt.Errorf("failed to monitor recovery mode: %s", err)
	}

	// os.Remove("/data/postgresql/recovery.signal")

	// Open read/write connection
	conn, err := openConn(ctx, node.PrivateIP, false)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Drop repmgr database to clear any metadata that belonged to the old cluster.
	_, err = conn.Exec(ctx, "DROP DATABASE repmgr;")
	if err != nil {
		return fmt.Errorf("failed to drop repmgr database: %s", err)
	}

	// Ensure auth is configured to match the environment.
	if err := node.setupCredentials(ctx, conn); err != nil {
		return fmt.Errorf("failed creating required users: %s", err)
	}

	// Stop the postgres process
	svisor.Stop()

	// Revert back to the original config file
	if err := restoreHBAFile(); err != nil {
		return fmt.Errorf("failed to restore original pg_hba.conf: %s", err)
	}

	if err := conn.Close(ctx); err != nil {
		return fmt.Errorf("failed to close connection: %s", err)
	}

	return nil

}

func (b *BarmanRestore) RestoreFromBackup(ctx context.Context) error {
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
		backupID, err = b.resolveBackupFromID(backups, b.recoveryTargetName)
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by id: %s", err)
		}
	case b.recoveryTargetTime != "":
		backupID, err = b.resolveBackupFromTime(backups, b.recoveryTargetTime)
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by time: %s", err)
		}
	case b.recoveryTarget != "":
		backupID, err = b.resolveBackupFromTime(backups, time.Now().String())
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
	if _, err := b.RestoreBackup(ctx, backupID); err != nil {
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

func (b *BarmanRestore) waitOnRecovery(ctx context.Context, privateIP string) error {
	conn, err := openConn(ctx, privateIP, false)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	timeout := time.After(10 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout:
			return fmt.Errorf("timed out waiting for PG to exit recovery mode")
		case <-ticker.C:
			var inRecovery bool
			if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery();").Scan(&inRecovery); err != nil {
				return fmt.Errorf("failed to check recovery status: %w", err)
			}
			if !inRecovery {
				return nil
			}
		}
	}
}
