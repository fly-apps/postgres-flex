package flypg

import (
	"fmt"
	"os"
	"time"
)

type FlyPGConfig struct {
	internalConfigFilePath string
	userConfigFilePath     string

	internalConfig ConfigMap
	userConfig     ConfigMap

	configPath string
}

func (c *FlyPGConfig) SetDefaults() {
	c.internalConfig = ConfigMap{
		"deadMemberRemovalThreshold": time.Hour * 24,
	}
}

func NewInternalConfig(configPath string) *FlyPGConfig {
	return &FlyPGConfig{
		internalConfigFilePath: fmt.Sprintf("%s/flypg.internal.conf", configPath),
		userConfigFilePath:     fmt.Sprintf("%s/flypg.user.conf", configPath),
		configPath:             configPath,
		internalConfig:         ConfigMap{},
		userConfig:             ConfigMap{},
	}
}
func (c *FlyPGConfig) InternalConfig() ConfigMap {
	return c.internalConfig
}

func (c *FlyPGConfig) UserConfig() ConfigMap {
	return c.userConfig
}

func (*FlyPGConfig) ConsulKey() string {
	return "FlyPGConfig"
}

func (c *FlyPGConfig) SetUserConfig(newConfig ConfigMap) {
	c.userConfig = newConfig
}

func (c *FlyPGConfig) InternalConfigFile() string {
	return c.internalConfigFilePath
}

func (c *FlyPGConfig) UserConfigFile() string {
	return c.userConfigFilePath
}

func (c *FlyPGConfig) CurrentConfig() (ConfigMap, error) {
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

func (c *FlyPGConfig) initialize() error {
	c.SetDefaults()

	file, err := os.Create(c.internalConfigFilePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %s", err)
	} else if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %s", err)
	}

	file, err = os.Create(c.userConfigFilePath)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return file.Sync()
}
