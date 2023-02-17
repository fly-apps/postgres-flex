package flypg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
)

type PGConfig struct {
	configFilePath string

	internalConfigFilePath string
	userConfigFilePath     string
	port                   int
	dataDir                string

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
	return c.internalConfigFilePath
}

func (c *PGConfig) UserConfigFile() string {
	return c.userConfigFilePath
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

func NewConfig(dataDir string, port int) *PGConfig {
	return &PGConfig{
		dataDir:        dataDir,
		port:           port,
		configFilePath: fmt.Sprintf("%s/postgresql.conf", dataDir),

		internalConfigFilePath: fmt.Sprintf("%s/postgresql.internal.conf", dataDir),
		userConfigFilePath:     fmt.Sprintf("%s/postgresql.user.conf", dataDir),

		internalConfig: ConfigMap{},
		userConfig:     ConfigMap{},
	}
}

// Print outputs the internal/user config to stdout.
func (c *PGConfig) Print(w io.Writer) error {
	internalCfg, err := ReadFromFile(c.internalConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	userCfg, err := ReadFromFile(c.userConfigFilePath)
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

// SetDefaults WriteDefaults will resolve the default configuration settings and write them to the
// internal config file.
func (c *PGConfig) SetDefaults() error {
	// The default wal_segment_size in mb
	const walSegmentSize = 16

	// Calculate total allocated disk in bytes
	diskSizeBytes, err := diskSizeInBytes()
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

	conf := ConfigMap{
		"random_page_cost":         "1.1",
		"port":                     c.port,
		"shared_buffers":           fmt.Sprintf("%dMB", sharedBuffersMb),
		"max_connections":          300,
		"max_replication_slots":    10,
		"min_wal_size":             fmt.Sprintf("%dMB", int(minWalMb)),
		"max_wal_size":             fmt.Sprintf("%dMB", int(maxWalMb)),
		"wal_compression":          "on",
		"wal_level":                "replica",
		"wal_log_hints":            true,
		"hot_standby":              true,
		"archive_mode":             true,
		"archive_command":          "'/bin/true'",
		"shared_preload_libraries": fmt.Sprintf("'%s'", strings.Join(sharedPreloadLibraries, ",")),
	}

	c.internalConfig = conf

	return nil
}

func (c *PGConfig) RuntimeApply(ctx context.Context, conn *pgx.Conn) error {
	for key, value := range c.userConfig {
		if err := admin.SetConfigurationSetting(ctx, conn, key, value); err != nil {
			fmt.Printf("failed to set configuration setting %s -> %s: %s", key, value, err)
		}
	}

	return nil
}

// initialize will ensure the required configuration files are stubbed and the parent
// postgresql.conf file includes them.
func (c *PGConfig) initialize() error {
	if _, err := os.Stat(c.internalConfigFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := utils.RunCommand(fmt.Sprintf("touch %s", c.internalConfigFilePath), "postgres"); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if _, err := os.Stat(c.userConfigFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := utils.RunCommand(fmt.Sprintf("touch %s", c.userConfigFilePath), "postgres"); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	b, err := os.ReadFile(c.configFilePath)
	if err != nil {
		return err
	}

	var entries []string
	if !strings.Contains(string(b), "postgresql.internal.conf") {
		entries = append(entries, "include 'postgresql.internal.conf'\n")
	}

	if !strings.Contains(string(b), "postgresql.user.conf") {
		entries = append(entries, "include 'postgresql.user.conf'\n")
	}

	if len(entries) > 0 {
		if err := c.writePGConfigEntries(entries); err != nil {
			return fmt.Errorf("failed to write pg entries: %s", err)
		}
	}

	if err := c.SetDefaults(); err != nil {
		return fmt.Errorf("failed to set pg defaults: %s", err)
	}

	return nil
}

func (c *PGConfig) writePGConfigEntries(entries []string) error {
	f, err := os.OpenFile(c.configFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil
	}
	defer f.Close()

	for _, entry := range entries {
		if _, err := f.WriteString(entry); err != nil {
			return fmt.Errorf("failed append configuration entry: %s", err)
		}
	}

	return f.Sync()
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

func diskSizeInBytes() (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/data", &stat); err != nil {
		return 0, err
	}
	return stat.Blocks * uint64(stat.Bsize), nil
}
