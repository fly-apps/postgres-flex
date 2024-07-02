package flypg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/fly-apps/postgres-flex/internal/utils"
)

type ConfigMap map[string]interface{}

type Config interface {
	InternalConfigFile() string
	UserConfigFile() string
	InternalConfig() ConfigMap
	UserConfig() ConfigMap
	SetUserConfig(configMap ConfigMap)
	ConsulKey() string
	CurrentConfig() (ConfigMap, error)
}

func WriteUserConfig(c Config, consul *state.Store) error {
	if c.UserConfig() == nil {
		return nil
	}

	if err := pushToConsul(c, consul); err != nil {
		return fmt.Errorf("failed to write to consul: %s", err)
	}

	if err := WriteConfigFiles(c); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
	}

	return nil
}

func PushUserConfig(c Config, consul *state.Store) error {
	if c.UserConfig() == nil {
		return nil
	}

	if err := pushToConsul(c, consul); err != nil {
		return fmt.Errorf("failed to write to consul: %s", err)
	}

	return nil
}

func SyncUserConfig(c Config, consul *state.Store) error {
	if os.Getenv("UNIT_TESTING") != "" {
		return nil
	}

	cfg, err := pullFromConsul(c, consul)
	if err != nil {
		return fmt.Errorf("failed to pull config from consul: %s", err)
	}
	if cfg == nil {
		return nil
	}

	c.SetUserConfig(cfg)

	if err := writeUserConfigFile(c); err != nil {
		return fmt.Errorf("failed to write user config: %s", err)
	}

	return nil
}

func pushToConsul(c Config, consul *state.Store) error {
	if c.UserConfig() == nil {
		return nil
	}

	configBytes, err := json.Marshal(c.UserConfig())
	if err != nil {
		return fmt.Errorf("failed to marshal user config: %s", err)
	}

	if err := consul.PushUserConfig(c.ConsulKey(), configBytes); err != nil {
		return fmt.Errorf("failed to push user config to consul: %s", err)
	}

	return nil
}

func pullFromConsul(c Config, consul *state.Store) (ConfigMap, error) {
	configBytes, err := consul.PullUserConfig(c.ConsulKey())
	if err != nil {
		return nil, err
	}
	if configBytes == nil {
		return nil, nil
	}

	var storeCfg ConfigMap
	if err = json.Unmarshal(configBytes, &storeCfg); err != nil {
		return nil, err
	}

	return storeCfg, nil
}

func WriteConfigFiles(c Config) error {
	if err := writeUserConfigFile(c); err != nil {
		return fmt.Errorf("failed to write user config: %s", err)
	}

	if err := writeInternalConfigFile(c); err != nil {
		return fmt.Errorf("failed to write internal config: %s", err)
	}

	return nil
}

func ReadFromFile(path string) (ConfigMap, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	conf := ConfigMap{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineArr := strings.Split(scanner.Text(), "=")
		key := strings.TrimSpace(lineArr[0])
		value := strings.TrimSpace(lineArr[1])
		conf[key] = value
	}

	return conf, nil
}

func writeInternalConfigFile(c Config) error {
	file, err := os.Create(c.InternalConfigFile())
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	internal := c.InternalConfig()

	for key, value := range internal {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		if _, err := file.Write([]byte(entry)); err != nil {
			return fmt.Errorf("failed to write to file: %s", err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %s", err)
	} else if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %s", err)
	}

	if os.Getenv("UNIT_TESTING") != "" {
		return nil
	}

	if err := utils.SetFileOwnership(c.InternalConfigFile(), "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership on %s: %s", c.InternalConfigFile(), err)
	}

	return nil
}

func writeUserConfigFile(c Config) error {
	file, err := os.Create(c.UserConfigFile())
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	for key, value := range c.UserConfig() {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		if _, err := file.Write([]byte(entry)); err != nil {
			return fmt.Errorf("failed to write to file: %s", err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %s", err)
	} else if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %s", err)
	}

	if os.Getenv("UNIT_TESTING") != "" {
		return nil
	}

	if err := utils.SetFileOwnership(c.UserConfigFile(), "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership on %s: %s", c.UserConfigFile(), err)
	}

	return nil
}
