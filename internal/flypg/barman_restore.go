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

	// Target parameters
	// For more information on these parameters, see:
	// https://www.postgresql.org/docs/current/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET
	recoveryTarget     string
	recoveryTargetName string
	recoveryTargetTime string

	recoveryTargetTimeline  string
	recoveryTargetAction    string
	recoveryTargetInclusive string
}

const (
	defaultRestoreDir     = "/data/postgresql"
	waitOnRecoveryTimeout = 10 * time.Minute
)

func NewBarmanRestore(configURL string) (*BarmanRestore, error) {
	// We only need access to the barman endpoints
	barman, err := NewBarman(nil, configURL, RestoreAuthProfile)
	if err != nil {
		return nil, fmt.Errorf("failed to create barman client: %s", err)
	}

	url, err := url.Parse(configURL)
	if err != nil {
		return nil, fmt.Errorf("invalid restore config url: %w", err)
	}

	restore := &BarmanRestore{Barman: barman}

	for key, value := range url.Query() {
		v := value[0]

		switch key {
		case "target":
			restore.recoveryTarget = v
		case "targetName":
			restore.recoveryTargetName = v
		case "targetInclusive":
			restore.recoveryTargetInclusive = v
		case "targetAction":
			restore.recoveryTargetAction = v
		case "targetTime":
			ts, err := formatTimestamp(v)
			if err != nil {
				return nil, fmt.Errorf("failed to parse target time: %s", err)
			}
			restore.recoveryTargetTime = ts
		case "targetTimeline":
			restore.recoveryTargetTimeline = v
		default:
			log.Printf("[WARN] unknown query parameter: %s. ignoring.", key)
		}
	}

	if restore.recoveryTargetAction == "" {
		restore.recoveryTargetAction = "promote"
	}

	return restore, nil
}

func (*BarmanRestore) walReplayAndReset(ctx context.Context, node *Node) error {
	// create a copy of the pg_hba.conf file so we can revert back to it when needed.
	if err := backupHBAFile(); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed backing up pg_hba.conf: %s", err)
	}

	// Grant local access so we can update internal credentials to match the environment.
	if err := grantLocalAccess(); err != nil {
		return fmt.Errorf("failed to grant local access: %s", err)
	}

	// Boot postgres and wait for WAL replay to complete
	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("restore", fmt.Sprintf("gosu postgres postgres -D /data/postgresql -p 5433 -h %s", node.PrivateIP))

	// Start the postgres process in the background.
	go func() {
		if err := svisor.Run(); err != nil {
			log.Printf("[ERROR] failed to boot postgres in the background: %s", err)
		}
	}()

	// Wait for the WAL replay to complete.
	if err := waitOnRecovery(ctx, node.PrivateIP); err != nil {
		return fmt.Errorf("failed to monitor recovery mode: %s", err)
	}

	// Open read/write connection
	conn, err := openConn(ctx, node.PrivateIP)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Drop repmgr database to clear any metadata that belonged to the old cluster.
	if _, err = conn.Exec(ctx, "DROP DATABASE repmgr;"); err != nil {
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

func (b *BarmanRestore) restoreFromBackup(ctx context.Context) error {
	backups, err := b.ListCompletedBackups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %s", err)
	}

	if len(backups.Backups) == 0 {
		return fmt.Errorf("no backups found")
	}

	var backupID string

	switch {
	case b.recoveryTarget != "":
		backupID, err = b.resolveBackupFromTime(backups, time.Now().Format(time.RFC3339))
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by time: %s", err)
		}
	case b.recoveryTargetTime != "":
		backupID, err = b.resolveBackupFromTime(backups, b.recoveryTargetTime)
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by time: %s", err)
		}
	case b.recoveryTargetName != "":
		// Resolve the target base backup
		backupID, err = b.resolveBackupFromName(backups, b.recoveryTargetName)
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by id/name: %s", err)
		}
	default:
		backupID, err = b.resolveBackupFromTime(backups, time.Now().Format(time.RFC3339))
		if err != nil {
			return fmt.Errorf("failed to resolve backup target by time: %s", err)
		}
	}

	if backupID == "" {
		return fmt.Errorf("no backup found")
	}

	// Download and restore the base backup
	if _, err := b.RestoreBackup(ctx, backupID); err != nil {
		return fmt.Errorf("failed to restore backup: %s", err)
	}

	// Write the recovery.signal file
	if err := os.WriteFile("/data/postgresql/recovery.signal", []byte(""), 0600); err != nil {
		return fmt.Errorf("failed to write recovery.signal: %s", err)
	}

	return nil
}

func (*BarmanRestore) resolveBackupFromName(backupList BackupList, name string) (string, error) {
	if len(backupList.Backups) == 0 {
		return "", fmt.Errorf("no backups found")
	}

	for _, backup := range backupList.Backups {
		// Allow for either the backup ID or the backup name to be used.
		if backup.ID == name || backup.Name == name {
			return backup.ID, nil
		}
	}

	return "", fmt.Errorf("no backup found with id/name %s", name)
}

func (*BarmanRestore) resolveBackupFromTime(backupList BackupList, restoreStr string) (string, error) {
	if len(backupList.Backups) == 0 {
		return "", fmt.Errorf("no backups found")
	}

	// Parse the restore string
	restoreTime, err := time.Parse(time.RFC3339, restoreStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse restore time: %s", err)
	}

	var lastBackupID string
	var lastBackupTime time.Time

	// This is the layout presented by barman
	layout := "Mon Jan 2 15:04:05 2006"

	for _, backup := range backupList.Backups {
		// TODO - This shouldn't be needed, but will keep it around until we can improve tests.
		if backup.Status != "DONE" {
			continue
		}

		// Parse the backup end time
		endTime, err := time.Parse(layout, backup.EndTime)
		if err != nil {
			return "", fmt.Errorf("failed to parse backup end time: %s", err)
		}

		// If the last backup ID is empty or the restore time is after the last backup time, update the last backup ID.
		if lastBackupID == "" || restoreTime.After(endTime) {
			lastBackupID = backup.ID
			lastBackupTime = endTime
		}

		// If the restore time is after the backup end time, we can short-circuit and return the last backup.
		if endTime.After(lastBackupTime) {
			return lastBackupID, nil
		}
	}

	return lastBackupID, nil
}

func waitOnRecovery(ctx context.Context, privateIP string) error {
	conn, err := openConn(ctx, privateIP)
	if err != nil {
		return fmt.Errorf("failed to establish connection to local node: %s", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// TODO - Figure out a more reasonable timeout to use here.
	timeout := time.After(waitOnRecoveryTimeout)
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
