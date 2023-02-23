package flypg

import (
	"fmt"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

type FlyConfig struct {
	internalConfigFilePath string
	userConfigFilePath     string

	internalConfig ConfigMap
	userConfig     ConfigMap
}

func (c *FlyConfig) InternalConfig() ConfigMap {
	return c.internalConfig
}

func (c *FlyConfig) UserConfig() ConfigMap {
	return c.userConfig
}

func (*FlyConfig) ConsulKey() string {
	return "FlyPGConfig"
}

func (c *FlyConfig) SetUserConfig(newConfig ConfigMap) {
	c.userConfig = newConfig
}

func (c *FlyConfig) InternalConfigFile() string {
	return c.internalConfigFilePath
}

func (c *FlyConfig) UserConfigFile() string {
	return c.userConfigFilePath
}

func (c *FlyConfig) SetDefaults() {
	c.internalConfig = ConfigMap{
		"deadMemberRemovalThreshold": time.Hour * 24,
	}
}

func (c *FlyConfig) CurrentConfig() (ConfigMap, error) {
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

func (c *FlyConfig) initialize(store *state.Store) error {
	c.SetDefaults()

	if err := SyncUserConfig(c, store); err != nil {
		return fmt.Errorf("failed to sync internal config from consul: %s", err)
	}

	if err := WriteConfigFiles(c); err != nil {
		return fmt.Errorf("failed to write internal config files: %s", err)
	}

	return nil
}
