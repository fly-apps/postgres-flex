package flypg

import (
	"testing"
)

const backupsResponse = `{
    "backups_list": [
        {
            "backup_label": "'START WAL LOCATION: 0/8000028 (file 000000010000000000000008)\\nCHECKPOINT LOCATION: 0/8000098\\nBACKUP METHOD: streamed\\nBACKUP FROM: primary\\nSTART TIME: 2024-06-25 19:44:13 UTC\\nLABEL: Barman backup cloud 20240625T194412\\nSTART TIMELINE: 1\\n'",
            "backup_name": "test-backup-2",
            "begin_offset": 40,
            "begin_time": "Tue Jun 25 19:44:12 2024",
            "begin_wal": "000000010000000000000008",
            "begin_xlog": "0/8000028",
            "compression": null,
            "config_file": "/data/postgresql/postgresql.conf",
            "copy_stats": {
                "total_time": 8.527192,
                "number_of_workers": 2,
                "analysis_time": 0,
                "analysis_time_per_item": {
                    "data": 0
                },
                "copy_time_per_item": {
                    "data": 0.624873
                },
                "serialized_copy_time_per_item": {
                    "data": 0.430501
                },
                "copy_time": 0.624873,
                "serialized_copy_time": 0.430501
            },
            "deduplicated_size": null,
            "end_offset": 312,
            "end_time": "Tue Jun 25 19:44:18 2024",
            "end_wal": "000000010000000000000008",
            "end_xlog": "0/8000138",
            "error": null,
            "hba_file": "/data/postgresql/pg_hba.conf",
            "ident_file": "/data/postgresql/pg_ident.conf",
            "included_files": [
                "/data/postgresql/postgresql.internal.conf"
            ],
            "mode": null,
            "pgdata": "/data/postgresql",
            "server_name": "cloud",
            "size": null,
            "status": "DONE",
            "systemid": "7384497274230341974",
            "tablespaces": null,
            "timeline": 1,
            "version": 150006,
            "xlog_segment_size": 16777216,
            "backup_id": "20240625T194412"
        },
        {
            "backup_label": "'START WAL LOCATION: 0/11000238 (file 000000010000000000000011)\\nCHECKPOINT LOCATION: 0/11000270\\nBACKUP METHOD: streamed\\nBACKUP FROM: primary\\nSTART TIME: 2024-06-26 17:26:53 UTC\\nLABEL: Barman backup cloud 20240626T172443\\nSTART TIMELINE: 1\\n'",
            "begin_offset": 568,
            "begin_time": "Wed Jun 26 17:24:43 2024",
            "begin_wal": "000000010000000000000011",
            "begin_xlog": "0/11000238",
            "compression": null,
            "config_file": "/data/postgresql/postgresql.conf",
            "copy_stats": {
                "total_time": 142.572774,
                "number_of_workers": 2,
                "analysis_time": 0,
                "analysis_time_per_item": {
                    "data": 0
                },
                "copy_time_per_item": {
                    "data": 0.845993
                },
                "serialized_copy_time_per_item": {
                    "data": 0.545768
                },
                "copy_time": 0.845993,
                "serialized_copy_time": 0.545768
            },
            "deduplicated_size": null,
            "end_offset": 840,
            "end_time": "Wed Jun 26 17:27:02 2024",
            "end_wal": "000000010000000000000011",
            "end_xlog": "0/11000348",
            "error": null,
            "hba_file": "/data/postgresql/pg_hba.conf",
            "ident_file": "/data/postgresql/pg_ident.conf",
            "included_files": [
                "/data/postgresql/postgresql.internal.conf"
            ],
            "mode": null,
            "pgdata": "/data/postgresql",
            "server_name": "cloud",
            "size": null,
            "status": "DONE",
            "systemid": "7384497274230341974",
            "tablespaces": null,
            "timeline": 1,
            "version": 150006,
            "xlog_segment_size": 16777216,
            "backup_id": "20240626T172443"
        }
    ]
}`

func TestParseBackups(t *testing.T) {
	t.Run("parseBackups", func(t *testing.T) {
		setRestoreDefaultEnv(t)

		restore, err := NewBarmanRestore()
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		list, err := restore.parseBackups([]byte(backupsResponse))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(list.Backups) != 2 {
			t.Fatalf("expected 2 backups, got %d", len(list.Backups))
		}

		firstBackup := list.Backups[0]
		if firstBackup.BackupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", firstBackup.BackupID)
		}

		if firstBackup.StartTime != "Tue Jun 25 19:44:12 2024" {
			t.Fatalf("expected start time to be Tue Jun 25 19:44:12 2024, got %s", firstBackup.StartTime)
		}

		if firstBackup.EndTime != "Tue Jun 25 19:44:18 2024" {
			t.Fatalf("expected end time to be Tue Jun 25 19:44:18 2024, got %s", firstBackup.EndTime)
		}

		secondBackup := list.Backups[1]

		if secondBackup.BackupID != "20240626T172443" {
			t.Fatalf("expected backup ID to be 20240626T172443, got %s", secondBackup.BackupID)
		}

		if secondBackup.StartTime != "Wed Jun 26 17:24:43 2024" {
			t.Fatalf("expected start time to be Wed Jun 26 17:24:43 2024, got %s", secondBackup.StartTime)
		}

		if secondBackup.EndTime != "Wed Jun 26 17:27:02 2024" {
			t.Fatalf("expected end time to be Wed Jun 26 17:27:02 2024, got %s", secondBackup.EndTime)
		}
	})
}

func TestResolveBackupTarget(t *testing.T) {
	setRestoreDefaultEnv(t)

	restore, err := NewBarmanRestore()
	if err != nil {
		t.Fatalf("NewBarmanRestore failed with: %v", err)
	}

	list, err := restore.parseBackups([]byte(backupsResponse))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("resolve-latest-backup-target", func(t *testing.T) {
		backupID, err := restore.resolveBackupTarget(list, "latest")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240626T172443" {
			t.Fatalf("expected backup ID to be 20240626T172443, got %s", backupID)
		}
	})

	t.Run("resolve-earliest-backup-target", func(t *testing.T) {
		backupID, err := restore.resolveBackupTarget(list, "2024-06-25T19:44:12Z")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", backupID)
		}
	})

	// "begin_time": "Tue Jun 25 19:44:12 2024"
	// "end_time": "Tue Jun 25 19:44:18 2024",
	t.Run("resolve-backup-within-first-window", func(t *testing.T) {
		backupID, err := restore.resolveBackupTarget(list, "2024-06-25T19:44:15Z")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", backupID)
		}
	})

	// "begin_time": "Wed Jun 26 17:24:43 2024",
	// "end_time": "Wed Jun 26 17:27:02 2024",
	t.Run("resolve-backup-within-second-window", func(t *testing.T) {
		backupID, err := restore.resolveBackupTarget(list, "2024-06-26T17:25:15Z")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240626T172443" {
			t.Fatalf("expected backup ID to be 20240626T172443, got %s", backupID)
		}
	})
}

func setRestoreDefaultEnv(t *testing.T) {
	t.Setenv("CLOUD_ARCHIVING_WAL_RESTORE", "true")
	t.Setenv("FLY_APP_NAME", "postgres-flex")
	t.Setenv("SOURCE_AWS_ACCESS_KEY_ID", "my-key")
	t.Setenv("SOURCE_AWS_SECRET_ACCESS_KEY", "my-secret")
	t.Setenv("SOURCE_AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev")
	t.Setenv("SOURCE_AWS_BUCKET_NAME", "my-bucket")
}