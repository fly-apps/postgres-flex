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
}

func WriteUserConfig(c Config, consul *state.Store) error {
	if c.UserConfig() != nil {
		if err := pushToConsul(c, consul); err != nil {
			return fmt.Errorf("failed to write to consul: %s", err)
		}

		if err := WriteConfigFiles(c); err != nil {
			return fmt.Errorf("failed to write to pg config file: %s", err)
		}
	}

	return nil
}

func PushUserConfig(c Config, consul *state.Store) error {
	if c.UserConfig() != nil {
		if err := pushToConsul(c, consul); err != nil {
			return fmt.Errorf("failed to write to consul: %s", err)
		}
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
		return err
	}

	if err := consul.PushUserConfig(c.ConsulKey(), configBytes); err != nil {
		return err
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
	internalFile, err := os.OpenFile(c.InternalConfigFile(), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer internalFile.Close()
	userFile, err := os.OpenFile(c.UserConfigFile(), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer userFile.Close()

	internal := c.InternalConfig()

	for key, value := range c.UserConfig() {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		if _, ok := internal[key]; ok {
			delete(internal, key)
		}
		userFile.Write([]byte(entry))
	}

	for key, value := range internal {
		entry := fmt.Sprintf("%s = %v\n", key, value)
		internalFile.Write([]byte(entry))
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

	return conf, nil
}
