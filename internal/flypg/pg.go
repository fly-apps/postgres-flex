package flypg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
)

type PGConfig struct {
	ConfigFilePath         string
	InternalConfigFilePath string
	UserConfigFilePath     string
	Port                   int
	DataDir                string

	barmanConfigPath string

	passwordFilePath string
	repmgrUsername   string
	repmgrDatabase   string

	internalConfig ConfigMap
	userConfig     ConfigMap
}

// type assertion
var _ Config = &PGConfig{}

func (c *PGConfig) InternalConfig() ConfigMap {
	return c.internalConfig
}

func (c *PGConfig) UserConfig() ConfigMap {
	return c.userConfig
}

func (*PGConfig) ConsulKey() string {
	return "PGConfig"
}

func (c *PGConfig) SetUserConfig(newConfig ConfigMap) {
	c.userConfig = newConfig
}

func (c *PGConfig) InternalConfigFile() string {
	return c.InternalConfigFilePath
}

func (c *PGConfig) UserConfigFile() string {
	return c.UserConfigFilePath
}

func (c *PGConfig) CurrentConfig() (ConfigMap, error) {
	internal, err := ReadFromFile(c.InternalConfigFile())
	if err != nil {
		return nil, err
	}
	user, err := ReadFromFile(c.UserConfigFile())
	if err != nil {
		return nil, err
	}

	all := ConfigMap{}

	for k, v := range internal {
		all[k] = v
	}
	for k, v := range user {
		all[k] = v
	}

	return all, nil
}

// Print outputs the internal/user config to stdout.
func (c *PGConfig) Print(w io.Writer) error {
	internalCfg, err := ReadFromFile(c.InternalConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	userCfg, err := ReadFromFile(c.UserConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	cfg := ConfigMap{}

	for k, v := range internalCfg {
		cfg[k] = v
	}

	for k, v := range userCfg {
		cfg[k] = v
	}

	e := json.NewEncoder(w)
	e.SetIndent("", "    ")

	return e.Encode(cfg)
}

func (c *PGConfig) SetDefaults(store *state.Store) error {
	// The default wal_segment_size in mb
	const walSegmentSize = 16

	// Calculate total allocated disk in bytes
	diskSizeBytes, err := diskSizeInBytes(c.DataDir)
	if err != nil {
		return fmt.Errorf("failed to fetch disk size: %s", err)
	}

	// Calculate total allocated memory in bytes
	memSizeInBytes, err := memTotalInBytes()
	if err != nil {
		return fmt.Errorf("failed to fetch total system memory: %s", err)
	}

	// Set max_wal_size to 10% of disk capacity.
	maxWalBytes := diskSizeBytes / 10
	maxWalMb := maxWalBytes / (1024 * 1024)

	// Set min_wal_size to 25% of max_wal_size
	minWalBytes := maxWalBytes / 4
	minWalMb := minWalBytes / (1024 * 1024)

	// min_wal_size must be at least twice the size of wal_segment_size.
	if minWalMb < (walSegmentSize * 2) {
		minWalMb = walSegmentSize * 2
	}

	var sharedBuffersBytes int
	// If total memory is greater than or equal to 1GB
	if memSizeInBytes >= (1024 * 1024 * 1024) {
		// Set shared_buffers to 25% of available memory
		sharedBuffersBytes = int(memSizeInBytes) / 4
	} else {
		// Set shared buffers to 10% of available memory
		sharedBuffersBytes = int(memSizeInBytes) / 10
	}
	sharedBuffersMb := sharedBuffersBytes / (1024 * 1024)

	sharedPreloadLibraries := []string{"repmgr"}
	// preload timescaledb if enabled
	if os.Getenv("TIMESCALEDB_ENABLED") == "true" {
		sharedPreloadLibraries = append(sharedPreloadLibraries, "timescaledb")
	}

	c.internalConfig = ConfigMap{
		"listen_addresses":         "'*'",
		"random_page_cost":         "1.1",
		"port":                     c.Port,
		"shared_buffers":           fmt.Sprintf("%dMB", sharedBuffersMb),
		"max_connections":          300,
		"max_replication_slots":    10,
		"min_wal_size":             fmt.Sprintf("%dMB", int(minWalMb)),
		"max_wal_size":             fmt.Sprintf("%dMB", int(maxWalMb)),
		"wal_compression":          "on",
		"wal_level":                "replica",
		"wal_log_hints":            true,
		"hot_standby":              true,
		"shared_preload_libraries": fmt.Sprintf("'%s'", strings.Join(sharedPreloadLibraries, ",")),
	}

	// Set WAL Archive specific settings
	if err := c.setArchiveConfig(store); err != nil {
		return fmt.Errorf("failed to set archive config: %s", err)
	}

	// Set recovery target settings
	if err := c.setRecoveryTargetConfig(); err != nil {
		return fmt.Errorf("failed to set recovery target config: %s", err)
	}

	return nil
}

func (c *PGConfig) setArchiveConfig(store *state.Store) error {
	if os.Getenv("S3_ARCHIVE_CONFIG") == "" {
		c.internalConfig["archive_mode"] = "off"
		return nil
	}

	barman, err := NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), DefaultAuthProfile)
	if err != nil {
		return fmt.Errorf("failed to initialize barman instance: %s", err)
	}

	if err := barman.LoadConfig(c.barmanConfigPath); err != nil {
		return fmt.Errorf("failed to load barman config: %s", err)
	}

	c.internalConfig["archive_mode"] = "on"
	c.internalConfig["archive_command"] = fmt.Sprintf("'%s'", barman.walArchiveCommand())
	c.internalConfig["archive_timeout"] = barman.Settings.ArchiveTimeout

	return nil
}

func (c *PGConfig) setRecoveryTargetConfig() error {
	if os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG") == "" {
		return nil
	}

	barmanRestore, err := NewBarmanRestore(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"))
	if err != nil {
		return err
	}

	// Set restore command and associated recovery target settings
	c.internalConfig["restore_command"] = fmt.Sprintf("'%s'", barmanRestore.walRestoreCommand())
	c.internalConfig["recovery_target_action"] = barmanRestore.recoveryTargetAction

	if barmanRestore.recoveryTargetTimeline != "" {
		c.internalConfig["recovery_target_timeline"] = barmanRestore.recoveryTargetTimeline
	}

	if barmanRestore.recoveryTargetInclusive != "" {
		c.internalConfig["recovery_target_inclusive"] = barmanRestore.recoveryTargetInclusive
	}

	switch {
	case barmanRestore.recoveryTarget != "":
		c.internalConfig["recovery_target"] = barmanRestore.recoveryTarget
	case barmanRestore.recoveryTargetName != "":
		c.internalConfig["recovery_target_name"] = fmt.Sprintf("barman_%s", barmanRestore.recoveryTargetName)
	case barmanRestore.recoveryTargetTime != "":
		c.internalConfig["recovery_target_time"] = fmt.Sprintf("'%s'", barmanRestore.recoveryTargetTime)
	}

	return nil
}

func (c *PGConfig) RuntimeApply(ctx context.Context, conn *pgx.Conn) error {
	for key, value := range c.userConfig {
		if err := admin.SetConfigurationSetting(ctx, conn, key, value); err != nil {
			log.Printf("[WARN] Failed to set configuration setting %s -> %s: %s", key, value, err)
		}
	}

	return nil
}

func (c *PGConfig) initdb() error {
	cmdStr := fmt.Sprintf("initdb --pgdata=%s --pwfile=%s", c.DataDir, c.passwordFilePath)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to init postgres: %s", err)
	}

	return nil
}

func (c *PGConfig) isInitialized() bool {
	_, err := os.Stat(c.DataDir)
	return !os.IsNotExist(err)
}

// initialize will ensure the required configuration files are stubbed and the parent
// postgresql.conf file includes them.
func (c *PGConfig) initialize(store *state.Store) error {
	if err := c.setDefaultHBA(); err != nil {
		return fmt.Errorf("failed updating pg_hba.conf: %s", err)
	}

	contents, err := os.ReadFile(c.ConfigFilePath)
	if err != nil {
		return err
	}

	var entries []string
	if !strings.Contains(string(contents), "postgresql.internal.conf") {
		entries = append(entries, "include 'postgresql.internal.conf'\n")
	}

	if !strings.Contains(string(contents), "postgresql.user.conf") {
		entries = append(entries, "include 'postgresql.user.conf'\n")
	}

	if len(entries) > 0 {
		if err := c.writePGConfigEntries(entries); err != nil {
			return fmt.Errorf("failed to write pg entries: %s", err)
		}
	}

	if err := c.SetDefaults(store); err != nil {
		return fmt.Errorf("failed to set pg defaults: %s", err)
	}

	if err := SyncUserConfig(c, store); err != nil {
		log.Printf("[WARN] Failed to sync user config from consul for postgres: %s\n", err.Error())
		log.Println("[WARN] This may cause this node to behave unexpectedly")
		if err := writeInternalConfigFile(c); err != nil {
			return fmt.Errorf("failed to write pg config files: %s", err)
		}
	} else {
		if err := WriteConfigFiles(c); err != nil {
			return fmt.Errorf("failed to write pg config files: %s", err)
		}
	}

	return nil
}

func (c *PGConfig) writePGConfigEntries(entries []string) error {
	file, err := os.OpenFile(c.ConfigFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	for _, entry := range entries {
		if _, err := file.WriteString(entry); err != nil {
			return fmt.Errorf("failed append configuration entry: %s", err)
		}
	}

	return file.Sync()
}

func (c *PGConfig) writePasswordFile(pwd string) error {
	if err := os.WriteFile(c.passwordFilePath, []byte(pwd), 0600); err != nil {
		return fmt.Errorf("failed to write default password to %s: %s", c.passwordFilePath, err)
	}

	if err := utils.SetFileOwnership(c.passwordFilePath, "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership: %s", err)
	}

	return nil
}

func (c *PGConfig) Validate(ctx context.Context, conn *pgx.Conn, requested ConfigMap) (ConfigMap, error) {
	if err := admin.ValidatePGSettings(ctx, conn, requested); err != nil {
		return requested, err
	}

	return c.validateCompatibility(requested)
}

func (c *PGConfig) validateCompatibility(requested ConfigMap) (ConfigMap, error) {
	current, err := c.CurrentConfig()
	if err != nil {
		return requested, fmt.Errorf("failed to resolve current config: %s", err)
	}

	// Shared preload libraries
	if v, ok := requested["shared_preload_libraries"]; ok {
		val := v.(string)

		// Remove any formatting that may be applied
		val = strings.Trim(val, "'")
		val = strings.TrimSpace(val)
		if val == "" {
			return requested, errors.New("`shared_preload_libraries` must contain the `repmgr` extension")
		}

		// Confirm repmgr is specified
		repmgrPresent := false
		entries := strings.Split(val, ",")
		for _, entry := range entries {
			if entry == "repmgr" {
				repmgrPresent = true
				break
			}
		}

		if !repmgrPresent {
			return requested, errors.New("`shared_preload_libraries` must contain the `repmgr` extension")
		}

		// Reconstruct value with proper formatting
		requested["shared_preload_libraries"] = fmt.Sprintf("'%s'", val)
	}

	// Wal-level
	if v, ok := requested["wal_level"]; ok {
		value := v.(string)
		switch value {
		case "minimal":
			var maxWalSenders int64

			// flyctl passes in `max_wal_senders` in as a string.
			maxWalSendersInterface := resolveConfigValue(requested, current, "max_wal_senders", "10")

			// Convert string to int
			maxWalSenders, err = strconv.ParseInt(maxWalSendersInterface.(string), 10, 64)
			if err != nil {
				return requested, fmt.Errorf("failed to parse max-wal-senders: %s", err)
			}

			if maxWalSenders > 0 {
				return requested, fmt.Errorf("max_wal_senders must be set to `0` before wal-level can be set to `minimal`")
			}

			archiveMode := resolveConfigValue(requested, current, "archive_mode", "off")
			if archiveMode.(string) != "off" {
				return requested, errors.New("archive_mode must be set to `off` before wal_level can be set to `minimal`")
			}

		case "replica", "logical":
			var maxWalSenders int64
			maxWalSendersInterface := resolveConfigValue(requested, current, "max_wal_senders", "10")

			// Convert string to int
			maxWalSenders, err = strconv.ParseInt(maxWalSendersInterface.(string), 10, 64)
			if err != nil {
				return requested, fmt.Errorf("failed to parse max-wal-senders: %s", err)
			}

			if maxWalSenders == 0 {
				return requested, fmt.Errorf("max_wal_senders must be greater than `0`")
			}
		}
	}

	// Max-wal-senders
	if v, ok := requested["max_wal_senders"]; ok {
		val := v.(string)

		// Convert string to int
		maxWalSenders, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return requested, fmt.Errorf("failed to parse max-wal-senders: %s", err)
		}

		walLevel := resolveConfigValue(requested, current, "wal_level", "replica")

		if maxWalSenders > 0 && walLevel == "minimal" {
			return requested, fmt.Errorf("max_wal_senders must be set to `0` when wal_level is `minimal`")
		}

		if maxWalSenders == 0 && walLevel != "minimal" {
			return requested, fmt.Errorf("max_wal_senders must be greater than `0` when wal_level is set to `%s`", walLevel.(string))
		}

	}

	// Max-replication-slots
	if v, ok := requested["max_replication_slots"]; ok {
		{
			val := v.(string)

			// Convert string to int
			maxReplicationSlots, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return requested, fmt.Errorf("failed to parse max-replication-slots: %s", err)
			}

			walLevel := resolveConfigValue(requested, current, "wal_level", "replica")

			if maxReplicationSlots > 0 && walLevel == "minimal" {
				return requested, fmt.Errorf("wal_level must be set to replica or higher before replication slots can be used")
			}
		}
	}

	// Max-connections
	if v, ok := requested["max_connections"]; ok {
		{
			const minConnections = 10

			val := v.(string)

			// Convert string to int
			maxConnections, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return requested, fmt.Errorf("failed to parse max-connections: %s", err)
			}

			if maxConnections < minConnections {
				return requested, fmt.Errorf("max_connections cannot be configured below %d", minConnections)
			}
		}
	}

	return requested, nil
}

func resolveConfigValue(requested ConfigMap, current ConfigMap, key string, defaultVal interface{}) interface{} {
	val := requested[key]
	if val == nil {
		val = current[key]
	}

	if val == nil {
		val = defaultVal
	}

	return val
}

type HBAEntry struct {
	Type     string
	Database string
	User     string
	Address  string
	Method   string
}

func (c *PGConfig) setDefaultHBA() error {
	entries := []HBAEntry{
		{
			Type:     "local",
			Database: "all",
			User:     "postgres",
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: "all",
			User:     "flypgadmin",
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: c.repmgrDatabase,
			User:     c.repmgrUsername,
			Method:   "trust",
		},
		{
			Type:     "local",
			Database: "replication",
			User:     c.repmgrUsername,
			Method:   "trust",
		},
		{
			Type:     "host",
			Database: "replication",
			User:     c.repmgrUsername,
			Address:  "fdaa::/16",
			Method:   "md5",
		},
		{
			Type:     "host",
			Database: fmt.Sprintf("replication,%s", c.repmgrDatabase),
			User:     c.repmgrUsername,
			Address:  "fdaa::/16",
			Method:   "md5",
		},
		{
			Type:     "host",
			Database: "all",
			User:     "all",
			Address:  "0.0.0.0/0",
			Method:   "md5",
		},
		{
			Type:     "host",
			Database: "all",
			User:     "all",
			Address:  "::0/0",
			Method:   "md5",
		},
	}

	path := fmt.Sprintf("%s/pg_hba.conf", c.DataDir)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("failed to create pg_hba.conf file: %s", err)
	}
	defer func() { _ = file.Close() }()

	for _, entry := range entries {
		str := fmt.Sprintf("%s %s %s %s %s\n", entry.Type, entry.Database, entry.User, entry.Address, entry.Method)
		_, err := file.WriteString(str)
		if err != nil {
			return err
		}
	}

	return file.Sync()
}

func memTotalInBytes() (int64, error) {
	memoryStr := os.Getenv("FLY_VM_MEMORY_MB")
	if memoryStr == "" {
		return 0, fmt.Errorf("FLY_VM_MEMORY_MB envvar has not been set")
	}

	parsed, err := strconv.ParseInt(memoryStr, 10, 64)
	if err != nil {
		return 0, err
	}

	memoryBytes := parsed * (1024 * 1024)

	return memoryBytes, nil
}

func diskSizeInBytes(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Blocks * uint64(stat.Bsize), nil
}
