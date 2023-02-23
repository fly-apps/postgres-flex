package flypg

import (
	"fmt"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

type FlyPGConfig struct {
	internalConfigFilePath string
	userConfigFilePath     string

	internalConfig ConfigMap
	userConfig     ConfigMap
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

func (c *FlyPGConfig) SetDefaults() {
	c.internalConfig = ConfigMap{
		"deadMemberRemovalThreshold": time.Hour * 24,
	}
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

func (c *FlyPGConfig) initialize(store *state.Store) error {
	c.SetDefaults()

	if err := SyncUserConfig(c, store); err != nil {
		return fmt.Errorf("failed to sync internal config from consul: %s", err)
	}

	if err := WriteConfigFiles(c); err != nil {
		return fmt.Errorf("failed to write internal config files: %s", err)
	}

	return nil
}
