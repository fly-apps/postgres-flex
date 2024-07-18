package flypg

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg/state"
)

type BarmanSettings struct {
	ArchiveTimeout      string `json:"archive_timeout,omitempty"`
	RecoveryWindow      string `json:"recovery_window,omitempty"`
	FullBackupFrequency string `json:"full_backup_frequency,omitempty"`
	MinimumRedundancy   string `json:"minimum_redundancy,omitempty"`
}

type BarmanConfig struct {
	internalConfigFilePath string
	userConfigFilePath     string
	internalConfig         ConfigMap
	userConfig             ConfigMap

	Settings BarmanSettings
}

const (
	barmanConsulKey        = "BarmanConfig"
	DefaultBarmanConfigDir = "/data/barman/"
)

// type assertion
var _ Config = &BarmanConfig{}

func (c *BarmanConfig) InternalConfig() ConfigMap         { return c.internalConfig }
func (c *BarmanConfig) UserConfig() ConfigMap             { return c.userConfig }
func (*BarmanConfig) ConsulKey() string                   { return barmanConsulKey }
func (c *BarmanConfig) SetUserConfig(newConfig ConfigMap) { c.userConfig = newConfig }
func (c *BarmanConfig) InternalConfigFile() string        { return c.internalConfigFilePath }
func (c *BarmanConfig) UserConfigFile() string            { return c.userConfigFilePath }

func NewBarmanConfig(store *state.Store, configDir string) (*BarmanConfig, error) {
	cfg := &BarmanConfig{
		internalConfigFilePath: configDir + "barman.internal.conf",
		userConfigFilePath:     configDir + "barman.user.conf",
	}

	if err := cfg.initialize(store, configDir); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *BarmanConfig) SetDefaults() {
	c.internalConfig = ConfigMap{
		"archive_timeout":       "60s",
		"recovery_window":       "7d",
		"full_backup_frequency": "24h",
		"minimum_redundancy":    "3",
	}
}

func (c *BarmanConfig) CurrentConfig() (ConfigMap, error) {
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

// ParseSettings reads the current config and returns the settings in a structured format.
func (c *BarmanConfig) ParseSettings() (BarmanSettings, error) {
	cfg, err := c.CurrentConfig()
	if err != nil {
		return BarmanSettings{}, fmt.Errorf("failed to read current config: %s", err)
	}

	recoveryWindow := fmt.Sprintf("RECOVERY WINDOW OF %s",
		convertRecoveryWindowDuration(cfg["recovery_window"].(string)))

	archiveTimeout, err := convertToPostgresUnits(cfg["archive_timeout"].(string))
	if err != nil {
		return BarmanSettings{}, fmt.Errorf("failed to convert archive_timeout to postgres units: %s", err)
	}

	return BarmanSettings{
		ArchiveTimeout:      archiveTimeout,
		RecoveryWindow:      recoveryWindow,
		FullBackupFrequency: cfg["full_backup_frequency"].(string),
		MinimumRedundancy:   cfg["minimum_redundancy"].(string),
	}, nil
}

func (c *BarmanConfig) Validate(requestedChanges map[string]interface{}) error {
	// Verify that the keys provided are valid
	for k := range requestedChanges {
		if _, ok := c.internalConfig[k]; !ok {
			return fmt.Errorf("invalid key: %s", k)
		}
	}

	for k, v := range requestedChanges {
		switch k {
		case "archive_timeout":
			// Ensure it can be converted to a Postgres duration
			if _, err := convertToPostgresUnits(v.(string)); err != nil {
				return fmt.Errorf("invalid value for archive_timeout: %v", err)
			}

		case "recovery_window":
			// Ensure that the value is a valid duration
			re := regexp.MustCompile(`^(\d+)([dwy])$`)
			matches := re.FindStringSubmatch(v.(string))
			if len(matches) != 3 {
				return fmt.Errorf("invalid value for recovery_window: %v", v)
			}

			num, err := strconv.Atoi(matches[1])
			if err != nil {
				return fmt.Errorf("failed to parse recovery_window: %w", err)
			}

			if num < 1 {
				return fmt.Errorf("invalid value for recovery_window (expected to be >= 1, got %v)", num)
			}

		case "full_backup_frequency":
			dur, err := time.ParseDuration(v.(string))
			if err != nil {
				return fmt.Errorf("invalid value for full_backup_frequency: %v", v)
			}

			if dur.Hours() < 1 {
				return fmt.Errorf("invalid value for full_backup_frequency (expected to be >= 1h, got %v)", dur)
			}
		case "minimum_redundancy":
			val, err := strconv.Atoi(v.(string))
			if err != nil {
				return fmt.Errorf("invalid value for minimum_redundancy: %v", v)
			}

			if val < 0 {
				return fmt.Errorf("invalid value for minimum_redundancy (expected be >= 0, got %v)", val)
			}
		}
	}

	return nil
}

func (c *BarmanConfig) initialize(store *state.Store, configDir string) error {
	// Ensure directory exists
	if err := os.MkdirAll(configDir, 0600); err != nil {
		return fmt.Errorf("failed to create barman config directory: %s", err)
	}

	c.SetDefaults()

	// Sync the user config from consul
	if err := SyncUserConfig(c, store); err != nil {
		log.Printf("[WARN] Failed to sync user config from consul for barman: %s\n", err.Error())
	}

	// Write the internal defaults
	if err := writeInternalConfigFile(c); err != nil {
		return fmt.Errorf("failed to write barman config files: %s", err)
	}

	// Create the user config file if it doesn't exist
	if _, err := os.Stat(c.UserConfigFile()); os.IsNotExist(err) {
		if _, err := os.Create(c.UserConfigFile()); err != nil {
			return fmt.Errorf("failed to stub user config file: %s", err)
		}
	}

	// Load the settings
	settings, err := c.ParseSettings()
	if err != nil {
		return fmt.Errorf("failed to parse barman config: %w", err)
	}

	c.Settings = settings

	return nil
}

func convertRecoveryWindowDuration(durationStr string) string {
	unitMap := map[string]string{
		"m": "MONTHS",
		"w": "WEEKS",
		"d": "DAYS",
	}
	for unit, text := range unitMap {
		if strings.HasSuffix(durationStr, unit) {
			return strings.TrimSuffix(durationStr, unit) + " " + text
		}
	}
	return durationStr
}

func convertToPostgresUnits(dStr string) (string, error) {
	// Use regex to split the numeric part and the unit
	re := regexp.MustCompile(`(\d+)([a-z]+)`)
	matches := re.FindStringSubmatch(dStr)
	if len(matches) != 3 {
		return "", fmt.Errorf("invalid duration format: %s", dStr)
	}

	// Parse the numeric value
	num, err := strconv.Atoi(matches[1])
	if err != nil {
		return "", err
	}

	// Map the Go units to Postgres units
	var postgresUnit string
	switch matches[2] {
	case "us":
		postgresUnit = "us"
	case "ms":
		postgresUnit = "ms"
	case "s":
		postgresUnit = "s"
	case "min", "m":
		postgresUnit = "min"
	case "h":
		postgresUnit = "h"
	case "d":
		postgresUnit = "d"
	default:
		return "", fmt.Errorf("unsupported postgres unit: %s", matches[2])
	}

	return fmt.Sprintf("%d%s", num, postgresUnit), nil
}
