package pg

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
	"github.com/fly-apps/postgres-flex/pkg/types"
	"github.com/fly-apps/postgres-flex/pkg/utils"
	"github.com/jackc/pgx/v4"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
)

type Config struct {
	configFilePath string

	internalConfigFilePath string
	userConfigFilePath     string
	dataDir                string

	internalConfig types.ConfigMap
	userConfig     types.ConfigMap
}

var _ types.ConfigModule = &Config{}

func NewConfig(dataDir string) *Config {
	return &Config{
		dataDir:        dataDir,
		configFilePath: fmt.Sprintf("%s/postgresql.conf", dataDir),

		internalConfigFilePath: fmt.Sprintf("%s/postgresql.internal.conf", dataDir),
		userConfigFilePath:     fmt.Sprintf("%s/postgresql.user.conf", dataDir),

		internalConfig: types.ConfigMap{},
		userConfig:     types.ConfigMap{},
	}
}

// Print outputs the internal/user config to stdout.
func (c *Config) Print(w io.Writer) error {
	internalCfg, err := c.pullFromFile(c.internalConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	userCfg, err := c.pullFromFile(c.userConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	cfg := types.ConfigMap{}

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

// Setup will ensure the required configuration files are stubbed and the parent
// postgresql.conf file includes them.
func (c *Config) Setup() error {
	if _, err := os.Stat(c.internalConfigFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := utils.RunCommand(fmt.Sprintf("touch %s", c.internalConfigFilePath)); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if _, err := os.Stat(c.userConfigFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := utils.RunCommand(fmt.Sprintf("touch %s", c.userConfigFilePath)); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	b, err := ioutil.ReadFile(c.configFilePath)
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
	}

	return nil
}

// WriteDefaults will resolve the default configuration settings and write them to the
// internal config file.
func (c *Config) WriteDefaults() error {
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

	conf := types.ConfigMap{
		"random_page_cost":         "1.1",
		"shared_buffers":           fmt.Sprintf("%dMB", sharedBuffersMb),
		"max_connections":          300,
		"max_replication_slots":    10,
		"min_wal_size":             fmt.Sprintf("%dMB", int(minWalMb)),
		"max_wal_size":             fmt.Sprintf("%dMB", int(maxWalMb)),
		"wal_compression":          "on",
		"wal_level":                "replica",
		"hot_standby":              true,
		"archive_mode":             true,
		"archive_command":          "'/bin/true'",
		"shared_preload_libraries": "repmgr",
	}

	if err := c.writeToFile(c.internalConfigFilePath, conf); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

// WriteUserConfig will push any user-defined configuration to Consul and write it to the user config file.
func (c *Config) WriteUserConfig(ctx context.Context, conn *pgx.Conn, consul *state.ConsulClient, cfg types.ConfigMap) error {
	if c.userConfig != nil {
		if err := c.pushToConsul(consul, cfg); err != nil {
			return fmt.Errorf("failed to write to consul: %s", err)
		}

		if err := c.writeToFile(c.userConfigFilePath, cfg); err != nil {
			return fmt.Errorf("failed to write to pg config file: %s", err)
		}
	}

	return nil
}

// SyncUserConfig will pull the latest user-defined configuration data from Consul and
// write it to the user config file.
func (c *Config) SyncUserConfig(ctx context.Context, consul *state.ConsulClient) error {
	cfg, err := c.pullFromConsul(consul)
	if err != nil {
		return fmt.Errorf("failed to pull config from consul: %s", err)
	}

	if err := c.writeToFile(c.userConfigFilePath, cfg); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

// ApplyUserConfigAtRuntime will take a config and attempt to set it at runtime.
func (c *Config) RuntimeApply(ctx context.Context, conn *pgx.Conn) error {
	for key, value := range c.userConfig {
		if err := admin.SetConfigurationSetting(ctx, conn, key, value); err != nil {
			fmt.Printf("failed to set configuration setting %s -> %s: %s", key, value, err)
		}
	}

	return nil
}

func (c *Config) pushToConsul(consul *state.ConsulClient, conf types.ConfigMap) error {
	if conf == nil {
		return nil
	}

	configBytes, err := json.Marshal(conf)
	if err != nil {
		return err
	}

	if err := consul.PushUserConfig(configBytes); err != nil {
		return err
	}

	return nil
}

func (c *Config) writeToFile(pathToFile string, conf types.ConfigMap) error {
	file, err := os.OpenFile(pathToFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range conf {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		file.Write([]byte(entry))
	}

	return nil
}

func (c *Config) pullFromFile(pathToFile string) (types.ConfigMap, error) {
	file, err := os.Open(pathToFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pgConf := types.ConfigMap{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineArr := strings.Split(scanner.Text(), "=")
		key := strings.TrimSpace(lineArr[0])
		value := strings.TrimSpace(lineArr[1])
		pgConf[key] = value
	}

	return pgConf, nil
}

func (c *Config) pullFromConsul(consul *state.ConsulClient) (types.ConfigMap, error) {
	configBytes, err := consul.PullUserConfig()
	if err != nil {
		return nil, err
	}

	var storeCfg types.ConfigMap
	if err = json.Unmarshal(configBytes, &storeCfg); err != nil {
		return nil, err
	}

	return storeCfg, nil
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
