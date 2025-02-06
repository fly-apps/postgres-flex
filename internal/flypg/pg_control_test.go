package flypg

import (
	"testing"
)

func TestParseSettingsFromFile(t *testing.T) {
	// Sample input that includes some lines with "setting:" and some without.
	input := `pg_control version number:            1300
Catalog version number:               202307071
Database system identifier:           7420479024646529412
Database cluster state:               in archive recovery
pg_control last modified:             Tue 04 Feb 2025 10:04:52 PM UTC
Latest checkpoint location:           2/40000060
Latest checkpoint's REDO location:    2/40000028
Latest checkpoint's REDO WAL file:    000000020000000200000040
Latest checkpoint's TimeLineID:       2
Latest checkpoint's PrevTimeLineID:   2
Latest checkpoint's full_page_writes: on
Latest checkpoint's NextXID:          0:34
wal_level setting:                    replica
wal_log_hints setting:                on
max_connections setting:              500
max_worker_processes setting:         8
Some other line without the keyword
Blocks per segment of large relation: 131072
WAL block size:                       8192
Bytes per WAL segment:                16777216
Maximum length of identifiers:        64
Maximum columns in an index:          32
Maximum size of a TOAST chunk:        1996
Size of a large-object chunk:         2048`

	settings, err := parsePGControlData(input)
	if err != nil {
		t.Fatalf("parsePGControlData returned an error: %v", err)
	}

	// Define the expected key/value pairs.
	expected := map[string]string{
		"wal_level":            "replica",
		"wal_log_hints":        "on",
		"max_connections":      "500",
		"max_worker_processes": "8",
	}

	if len(settings) != len(expected) {
		t.Errorf("expected %d settings, got %d", len(expected), len(settings))
	}

	// Verify that the expected key/value pairs are present in the settings map.
	for key, want := range expected {
		got, ok := settings[key]
		if !ok {
			t.Errorf("expected key %q not found in settings", key)
		} else if got != want {
			t.Errorf("for key %q, expected value %q, got %q", key, want, got)
		}
	}
}
