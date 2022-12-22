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
	configFilePath string

	internalConfigFilePath string
	userConfigFilePath     string
	dataDir                string

	internalConfig pgConfig
	userConfig     pgConfig
}

func NewConfig(dataDir string) *Config {
	return &Config{
		dataDir:        dataDir,
		configFilePath: fmt.Sprintf("%s/postgresql.conf", dataDir),

		internalConfigFilePath: fmt.Sprintf("%s/postgresql.internal.conf", dataDir),
		userConfigFilePath:     fmt.Sprintf("%s/postgresql.user.conf", dataDir),

		internalConfig: pgConfig{},
		userConfig:     pgConfig{},
	}
}

// Print will output the local configuration data to stdout.
func (c *Config) Print(w io.Writer) error {
	internalCfg, err := c.pullFromConfig(c.internalConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	userCfg, err := c.pullFromConfig(c.userConfigFilePath)
	if err != nil {
		return fmt.Errorf("failed to read internal config: %s", err)
	}

	cfg := pgConfig{}

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

// WriteInternalConfig will resolve the default configuration settings and write them to the
// internal config file.
func (c *Config) WriteDefaults() error {
	mem, err := memTotal()
	if err != nil {
		return fmt.Errorf("failed to fetch total system memory: %s", err)
	}

	conf := pgConfig{
		"shared_buffers":           fmt.Sprintf("%dMB", mem/4),
		"max_wal_senders":          10,
		"max_connections":          300,
		"wal_level":                "hot_standby",
		"hot_standby":              true,
		"archive_mode":             true,
		"archive_command":          "'/bin/true'",
		"shared_preload_libraries": "repmgr",
	}

	// Write configuration to local file.
	if err := c.writeToConfig(c.internalConfigFilePath, conf); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

// Setup will ensure the required configuration files are created and that the parent
// postgresql.conf file is including them.
func (c Config) Setup() error {
	if _, err := os.Stat(c.internalConfigFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := runCommand(fmt.Sprintf("touch %s", c.internalConfigFilePath)); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if _, err := os.Stat(c.userConfigFilePath); err != nil {
		if os.IsNotExist(err) {
			if err := runCommand(fmt.Sprintf("touch %s", c.internalConfigFilePath)); err != nil {
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
		entries = append(entries, "include 'postgresql.user.conf'")
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

// WriteUserConfig will push any user-defined configuration to Consul, the user configuration file
// and apply eligible changes at runtime.
func (c Config) WriteUserConfig(ctx context.Context, conn *pgx.Conn, consul *state.ConsulClient, cfg pgConfig) error {
	if c.userConfig != nil {
		if err := c.pushToConsul(consul, cfg); err != nil {
			return fmt.Errorf("failed to write to consul: %s", err)
		}

		// Write configuration to local file.
		if err := c.writeToConfig(c.userConfigFilePath, cfg); err != nil {
			return fmt.Errorf("failed to write to pg config file: %s", err)
		}

		// Attempt to set configurations ettings at runtime.
		if err := c.applyUserConfigAtRuntime(ctx, conn, cfg); err != nil {
			return fmt.Errorf("faield to write to pg runtime: %s", err)
		}
	}

	return nil
}

// OfflineUserConfigSync will pull the latest user-defined configuration data from Consul and
// write it to the user config file.
func (c Config) OfflineUserConfigSync(ctx context.Context, consul *state.ConsulClient) error {
	// Apply Consul configuration.
	cfg, err := c.pullConfigFromConsul(consul)
	if err != nil {
		return fmt.Errorf("failed to pull config from consul: %s", err)
	}

	// Write configuration to local file.
	if err := c.writeToConfig(c.userConfigFilePath, cfg); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

// UserConfigSync will pull the latest user-defined configuration from Consul,
// write it to the user config file and attempt to apply any new changes at runtime.
func (c Config) UserConfigSync(ctx context.Context, conn *pgx.Conn, consul *state.ConsulClient) error {
	// Apply Consul configuration.
	cfg, err := c.pullConfigFromConsul(consul)
	if err != nil {
		return fmt.Errorf("failed to pull config from consul: %s", err)
	}

	// Write configuration to local file.
	if err := c.writeToConfig(c.userConfigFilePath, cfg); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	fmt.Println("Applying config at runtime")
	// Attempt to set configuration settings at runtime.
	if err := c.applyUserConfigAtRuntime(ctx, conn, cfg); err != nil {
		return fmt.Errorf("faield to write to pg runtime: %s", err)
	}

	return nil
}

func (c Config) applyUserConfigAtRuntime(ctx context.Context, conn *pgx.Conn, conf pgConfig) error {
	for key, value := range conf {
		if err := admin.SetConfigurationSetting(ctx, conn, key, value); err != nil {
			fmt.Printf("failed to set configuration setting %s -> %s: %s", key, value, err)
		}
	}

	return nil
}

func (c Config) pushToConsul(consul *state.ConsulClient, conf pgConfig) error {
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

func (c Config) writeToConfig(pathToConfig string, conf pgConfig) error {
	file, err := os.OpenFile(pathToConfig, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
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

func (c *Config) pullFromConfig(pathToFile string) (pgConfig, error) {
	file, err := os.Open(pathToFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pgConf := pgConfig{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineArr := strings.Split(scanner.Text(), "=")
		key := strings.TrimSpace(lineArr[0])
		value := strings.TrimSpace(lineArr[1])
		pgConf[key] = value
	}

	return pgConf, nil
}

func (c Config) pullConfigFromConsul(consul *state.ConsulClient) (pgConfig, error) {
	configBytes, err := consul.PullUserConfig()
	if err != nil {
		return nil, err
	}

	var storeCfg pgConfig
	if err = json.Unmarshal(configBytes, &storeCfg); err != nil {
		return nil, err
	}

	return storeCfg, nil
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
