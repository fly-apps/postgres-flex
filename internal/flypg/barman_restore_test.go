package flypg

import (
	"fmt"
	"os"
	"testing"
)

const backupsResponse = `{
    "backups_list": [
		{
            "backup_label": null,
            "begin_offset": 216,
            "begin_time": "Tue Jun 24 19:44:20 2024",
            "begin_wal": "00000005000000000000002A",
            "begin_xlog": "0/2A0000D8",
            "compression": null,
            "config_file": "/data/postgresql/postgresql.conf",
            "copy_stats": null,
            "deduplicated_size": null,
            "end_offset": null,
            "end_time": null,
            "end_wal": null,
            "end_xlog": null,
            "error": "failure uploading data (connection already closed)",
            "hba_file": "/data/postgresql/pg_hba.conf",
            "ident_file": "/data/postgresql/pg_ident.conf",
            "included_files": [
                "/data/postgresql/postgresql.auto.conf",
                "/data/postgresql/postgresql.internal.conf"
            ],
            "mode": null,
            "pgdata": "/data/postgresql",
            "server_name": "cloud",
            "size": null,
            "status": "FAILED",
            "systemid": "7332222271544570189",
            "tablespaces": null,
            "timeline": 5,
            "version": 150006,
            "xlog_segment_size": 16777216,
            "backup_id": "20240702T210544"
        },
        {
            "backup_label": "'START WAL LOCATION: 0/8000028 (file 000000010000000000000008)\\nCHECKPOINT LOCATION: 0/8000098\\nBACKUP METHOD: streamed\\nBACKUP FROM: primary\\nSTART TIME: 2024-06-25 19:44:13 UTC\\nLABEL: Barman backup cloud 20240625T194412\\nSTART TIMELINE: 1\\n'",
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
			"backup_name": "test-backup-1",
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

func TestNewBarmanRestore(t *testing.T) {
	setRestoreDefaultEnv(t)
	t.Run("defaults", func(t *testing.T) {
		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		if restore.bucket != "my-bucket" {
			t.Fatalf("expected bucket to be my-bucket, got %s", restore.bucket)
		}

		if restore.BucketURL() != "s3://my-bucket" {
			t.Fatalf("expected bucket to be my-bucket, got %s", restore.bucket)
		}

		if restore.bucketDirectory != "my-directory" {
			t.Fatalf("expected bucket directory to be my-directory, got %s", restore.bucketDirectory)
		}

		if restore.appName != "postgres-flex" {
			t.Fatalf("expected app name to be postgres-flex, got %s", restore.appName)
		}

		if restore.provider != "aws-s3" {
			t.Fatalf("expected provider to be aws-s3, got %s", restore.provider)
		}

		if restore.endpoint != "https://fly.storage.tigris.dev" {
			t.Fatalf("expected endpoint to be https://fly.storage.tigris.dev, got %s", restore.endpoint)
		}

	})

	t.Run("target", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?target=immediate")

		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		if restore.recoveryTarget != "immediate" {
			t.Fatalf("expected recovery target to be 'immediate', got %s", restore.recoveryTarget)
		}
	})

	t.Run("target-time", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetTime=2024-07-03T17:55:22Z")

		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		if restore.recoveryTargetTime != "2024-07-03T17:55:22+00:00" {
			t.Fatalf("expected recovery target time to be 2024-07-03T17:55:22+00:00, got %s", restore.recoveryTargetTime)
		}
	})

	t.Run("target-name", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetName=20240705T051010")

		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		if restore.recoveryTargetName != "20240705T051010" {
			t.Fatalf("expected recovery target name to be 20240705T051010, got %s", restore.recoveryTargetName)
		}
	})

	t.Run("target-name-with-alias", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetName=test-backup-1")

		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		if restore.recoveryTargetName != "test-backup-1" {
			t.Fatalf("expected recovery target name to be test-backup-1, got %s", restore.recoveryTargetName)
		}
	})

	t.Run("target-name-with-options", func(t *testing.T) {
		t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory?targetName=20240705T051010&targetAction=shutdown&targetTimeline=2&targetInclusive=false")

		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		if restore.recoveryTargetName != "20240705T051010" {
			t.Fatalf("expected recovery target name to be 20240705T051010, got %s", restore.recoveryTargetName)
		}

		if restore.recoveryTargetAction != "shutdown" {
			t.Fatalf("expected recovery target action to be shutdown, got %s", restore.recoveryTargetAction)
		}

		if restore.recoveryTargetTimeline != "2" {
			t.Fatalf("expected recovery target timeline to be 2, got %s", restore.recoveryTargetTimeline)
		}

	})
}

func TestWALRestoreCommand(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	setRestoreDefaultEnv(t)
	restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := fmt.Sprintf("barman-cloud-wal-restore --cloud-provider aws-s3 --endpoint-url https://fly.storage.tigris.dev --profile restore s3://my-bucket my-directory %%f %%p")

	if restore.walRestoreCommand() != expected {
		t.Fatalf("expected WALRestoreCommand to be %s, but got %s", expected, restore.walRestoreCommand())
	}
}

func TestParseBackups(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	t.Run("parseBackups", func(t *testing.T) {
		setRestoreDefaultEnv(t)

		restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
		if err != nil {
			t.Fatalf("NewBarmanRestore failed with: %v", err)
		}

		list, err := restore.parseBackups([]byte(backupsResponse))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(list.Backups) != 3 {
			t.Fatalf("expected 2 backups, got %d", len(list.Backups))
		}

		t.Run("first-backup", func(t *testing.T) {
			backup := list.Backups[0]
			if backup.ID != "20240702T210544" {
				t.Fatalf("expected backup ID to be 20240625T194412, got %s", backup.ID)
			}

			if backup.StartTime != "Tue Jun 24 19:44:20 2024" {
				t.Fatalf("expected start time to be Tue Jun 24 19:44:20 2024, got %s", backup.StartTime)
			}

			if backup.EndTime != "" {
				t.Fatalf("expected end time to be empty, but got %s", backup.EndTime)
			}

			if backup.Status != "FAILED" {
				t.Fatalf("expected status to be FAILED, got %s", backup.Status)
			}

			if backup.Name != "" {
				t.Fatalf("expected name to be empty, but got %s", backup.Name)
			}

		})

		t.Run("second-backup", func(t *testing.T) {
			backup := list.Backups[1]
			if backup.Status != "DONE" {
				t.Fatalf("expected status to be DONE, got %s", backup.Status)
			}

			if backup.Name != "test-backup-1" {
				t.Fatalf("expected name to be test-backup-1, got %s", backup.Name)
			}
		})

		t.Run("third-backup", func(t *testing.T) {
			backup := list.Backups[2]

			if backup.ID != "20240626T172443" {
				t.Fatalf("expected backup ID to be 20240626T172443, got %s", backup.ID)
			}

			if backup.StartTime != "Wed Jun 26 17:24:43 2024" {
				t.Fatalf("expected start time to be Wed Jun 26 17:24:43 2024, got %s", backup.StartTime)
			}

			if backup.EndTime != "Wed Jun 26 17:27:02 2024" {
				t.Fatalf("expected end time to be Wed Jun 26 17:27:02 2024, got %s", backup.EndTime)
			}

		})
	})
}

func TestResolveBackupTarget(t *testing.T) {
	if err := setup(t); err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	setRestoreDefaultEnv(t)

	restore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
	if err != nil {
		t.Fatalf("NewBarmanRestore failed with: %v", err)
	}

	list, err := restore.parseBackups([]byte(backupsResponse))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("resolve-oldest-target", func(t *testing.T) {
		backupID, err := restore.resolveBackupFromTime(list, "2024-06-25T19:40:18-00:00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", backupID)
		}
	})

	t.Run("resolve-backup-within-first-window", func(t *testing.T) {
		backupID, err := restore.resolveBackupFromTime(list, "2024-06-25T19:44:15-00:00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", backupID)
		}
	})

	t.Run("resolve-backup-within-second-window", func(t *testing.T) {
		backupID, err := restore.resolveBackupFromTime(list, "2024-06-26T17:29:15-00:00")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240626T172443" {
			t.Fatalf("expected backup ID to be 20240626T172443, got %s", backupID)
		}
	})

	t.Run("resolve-backup-by-name", func(t *testing.T) {
		backupID, err := restore.resolveBackupFromName(list, "20240625T194412")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", backupID)
		}
	})

	t.Run("resolve-backup-by-name-with-alias", func(t *testing.T) {
		// resolve backup by alias
		backupID, err := restore.resolveBackupFromName(list, "test-backup-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if backupID != "20240625T194412" {
			t.Fatalf("expected backup ID to be 20240625T194412, got %s", backupID)
		}
	})
}

func setRestoreDefaultEnv(t *testing.T) {
	t.Setenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG", "https://my-key:my-secret@fly.storage.tigris.dev/my-bucket/my-directory")
	t.Setenv("FLY_APP_NAME", "postgres-flex")
}
