package flypg

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
	"github.com/jackc/pgx/v4"
	"github.com/shirou/gopsutil/v3/mem"
)

type pgConfig map[string]interface{}

type Config struct {
	configFilePath       string
	customConfigFilePath string
	dataDir              string

	pgConfig pgConfig
}

func NewConfig(dataDir string) *Config {
	return &Config{
		dataDir:              dataDir,
		configFilePath:       fmt.Sprintf("%s/postgresql.conf", dataDir),
		customConfigFilePath: fmt.Sprintf("%s/postgresql.custom.conf", dataDir),
		pgConfig:             pgConfig{},
	}
}

// SetDefaults will apply the default configuration settings to the config struct.
// Warning - it's important that this is called prior to any new settings, otherwise
// they may be overwritten.
func (c *Config) SetDefaults() error {
	mem, err := memTotal()
	if err != nil {
		return fmt.Errorf("failed to fetch total system memory: %s", err)
	}

	c.pgConfig = map[string]interface{}{
		"shared_buffers":           fmt.Sprintf("%dMB", mem/4),
		"max_wal_senders":          10,
		"max_replication_slots":    10,
		"max_connections":          300,
		"wal_level":                "hot_standby",
		"hot_standby":              true,
		"archive_mode":             true,
		"archive_command":          "'/bin/true'",
		"shared_preload_libraries": "repmgr",
	}

	return nil
}

// SaveOffline will write our configuration data to Consul and to our local configuration
// file. This is safe to run when Postgres is not running.
func (c Config) SaveOffline(consul *state.ConsulClient) error {
	// Don't persist an empty config
	if c.pgConfig == nil {
		return nil
	}
	// Push configuration to Consul.
	if err := c.pushToConsul(consul); err != nil {
		return fmt.Errorf("failed to write to consul: %s", err)
	}

	// Write configuration to local file.
	if err := c.writeToFile(); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

// SaveOnline will write our configuration information to Consul, local configuration
// and will attempt to apply eligible changes at runtime.
func (c Config) SaveOnline(ctx context.Context, conn *pgx.Conn, consul *state.ConsulClient) error {
	// Don't persist an empty config
	if c.pgConfig == nil {
		return fmt.Errorf("unable to save an empty config")
	}

	if err := c.SaveOffline(consul); err != nil {
		return err
	}

	// Attempt to set configurations ettings at runtime.
	if err := c.applyPGConfigAtRuntime(ctx, conn); err != nil {
		return fmt.Errorf("faield to write to pg runtime: %s", err)
	}

	return nil
}

// SyncOffline will pull the latest Postgres configuration information from Consul and
// write it to our local configuration file.
func (c *Config) SyncOffline(ctx context.Context, consul *state.ConsulClient) error {
	// Apply Consul configuration.
	if err := c.pullConfigFromConsul(consul); err != nil {
		return fmt.Errorf("failed to pull config from consul: %s", err)
	}
	// Write configuration to local file.
	if err := c.writeToFile(); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

// SyncOnline will pull the latest Postgres configuration information from Consul and
// write it to our local configuration file and attempt to apply any new changes at runtime.
func (c *Config) SyncOnline(ctx context.Context, conn *pgx.Conn, consul *state.ConsulClient) error {
	if err := c.SyncOffline(ctx, consul); err != nil {
		return err
	}

	fmt.Println("Applying config at runtime")
	// Attempt to set configuration settings at runtime.
	if err := c.applyPGConfigAtRuntime(ctx, conn); err != nil {
		return fmt.Errorf("faield to write to pg runtime: %s", err)
	}

	return nil
}

// Print will output the local configuration data to stdout.
func (c *Config) Print(w io.Writer) error {
	if err := c.SetDefaults(); err != nil {
		return err
	}
	if err := c.pullFromFile(); err != nil {
		return err
	}

	e := json.NewEncoder(w)
	e.SetIndent("", "    ")

	return e.Encode(c.pgConfig)
}

func (c Config) EnableCustomConfig() error {
	if err := runCommand(fmt.Sprintf("touch %s", c.customConfigFilePath)); err != nil {
		return err
	}

	// read the whole file at once
	b, err := ioutil.ReadFile(c.configFilePath)
	if err != nil {
		return err
	}

	if strings.Contains(string(b), "postgresql.custom.conf") {
		return nil
	}

	f, err := os.OpenFile(c.configFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil
	}
	defer f.Close()

	if _, err := f.WriteString("include 'postgresql.custom.conf'"); err != nil {
		return fmt.Errorf("failed append to conf file: %s", err)
	}

	return nil
}

func (c Config) applyPGConfigAtRuntime(ctx context.Context, conn *pgx.Conn) error {
	for key, value := range c.pgConfig {
		if err := admin.SetConfigurationSetting(ctx, conn, key, value); err != nil {
			fmt.Printf("failed to set configuration setting %s -> %s: %s", key, value, err)
		}
	}

	return nil
}

func (c Config) pushToConsul(consul *state.ConsulClient) error {
	configBytes, err := json.Marshal(c.pgConfig)
	if err != nil {
		return err
	}

	if consul == nil {
		consul, err = state.NewConsulClient()
		if err != nil {
			return err
		}
	}

	if err := consul.PushConfig(configBytes); err != nil {
		return err
	}

	return nil
}

func (c Config) writeToFile() error {
	file, err := os.OpenFile(c.customConfigFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range c.pgConfig {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		file.Write([]byte(entry))
	}

	return nil
}

func (c *Config) pullFromFile() error {
	file, err := os.Open(c.customConfigFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineArr := strings.Split(scanner.Text(), "=")
		key := strings.TrimSpace(lineArr[0])
		value := strings.TrimSpace(lineArr[1])
		c.pgConfig[key] = value
	}

	return nil
}

func (c *Config) pullConfigFromConsul(consul *state.ConsulClient) error {
	configBytes, err := consul.PullConfig()
	if err != nil {
		return err
	}

	var storeCfg pgConfig
	if err = json.Unmarshal(configBytes, &storeCfg); err != nil {
		return err
	}

	for key, value := range storeCfg {
		c.pgConfig[key] = value
	}

	return nil
}

func memTotal() (memoryMb int64, err error) {
	if raw := os.Getenv("FLY_VM_MEMORY_MB"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, err
		}
		memoryMb = parsed
	}

	if memoryMb == 0 {
		v, err := mem.VirtualMemory()
		if err != nil {
			return 0, err
		}
		memoryMb = int64(v.Total / 1024 / 1024)
	}

	return
}
