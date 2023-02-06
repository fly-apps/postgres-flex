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
		"standby_clean_interval": time.Hour * 24,
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

func (c *FlyPGConfig) ConsulKey() string {
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

func (c *FlyPGConfig) CurrentConfig() (map[string]interface{}, error) {
	internal, err := ReadFromFile(c.InternalConfigFile())
	if err != nil {
		return nil, err
	}
	user, err := ReadFromFile(c.UserConfigFile())
	if err != nil {
		return nil, err
	}

	all := map[string]interface{}{}

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

	internal, err := os.Create(c.internalConfigFilePath)
	if err != nil {
		return err
	}
	defer internal.Close()

	user, err := os.Create(c.userConfigFilePath)
	if err != nil {
		return err
	}
	defer user.Close()

	return nil
}
