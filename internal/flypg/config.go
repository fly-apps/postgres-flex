package flypg

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
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
	cfg, err := pullFromConsul(c, consul)
	if err != nil {
		return fmt.Errorf("failed to pull config from consul: %s", err)
	}
	if cfg == nil {
		return nil
	}
	c.SetUserConfig(cfg)

	if err := WriteConfigFiles(c); err != nil {
		return fmt.Errorf("failed to write to pg config file: %s", err)
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
	defer file.Close()

	conf := ConfigMap{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineArr := strings.Split(scanner.Text(), "=")
		key := strings.TrimSpace(lineArr[0])
		value := strings.TrimSpace(lineArr[1])
		conf[key] = value
	}

	return conf, file.Sync()
}

func writeInternalConfigFile(c Config) error {
	file, err := os.OpenFile(c.InternalConfigFile(), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	internal := c.InternalConfig()

	for key, value := range internal {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		file.Write([]byte(entry))
	}

	return file.Sync()
}

func writeUserConfigFile(c Config) error {
	file, err := os.OpenFile(c.UserConfigFile(), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	internal := c.InternalConfig()

	for key, value := range c.UserConfig() {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		delete(internal, key)
		file.Write([]byte(entry))
	}

	return file.Sync()
}
