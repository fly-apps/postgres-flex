package flypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/fly-apps/postgres-flex/internal/utils"

	"github.com/jackc/pgx/v5"
)

const (
	transactionPooler = "transaction"
	sessionPooler     = "session"
	statementPooler   = "statement"
)

type PGBouncer struct {
	PrivateIP   string
	Credentials Credentials
	ConfigPath  string
	Port        int
	ForwardPort int

	internalConfig ConfigMap
	userConfig     ConfigMap
}

func (p *PGBouncer) InternalConfigFile() string {
	return fmt.Sprintf("%s/pgbouncer.internal.ini", p.ConfigPath)
}

func (p *PGBouncer) UserConfigFile() string {
	return fmt.Sprintf("%s/pgbouncer.user.ini", p.ConfigPath)
}

func (p *PGBouncer) InternalConfig() ConfigMap {
	return p.internalConfig
}

func (p *PGBouncer) UserConfig() ConfigMap {
	return p.userConfig
}

func (p *PGBouncer) SetUserConfig(configMap ConfigMap) {
	p.userConfig = configMap
}

func (p *PGBouncer) ConsulKey() string {
	return "PGBouncer"
}

func (p *PGBouncer) ConfigurePrimary(ctx context.Context, primary string, reload bool) error {
	path := fmt.Sprintf("%s/pgbouncer.database.ini", p.ConfigPath)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	contents := fmt.Sprintf("[databases]\n* = host=%s port=%d\n", primary, p.ForwardPort)
	_, err = file.Write([]byte(contents))
	if err != nil {
		return err
	}

	if reload {
		err = p.reloadConfig(ctx)
		if err != nil {
			return fmt.Errorf("failed to reconfigure pgbouncer primary: %s", err)
		}
	}
	return nil
}

func (p *PGBouncer) CurrentConfig() (ConfigMap, error) {
	internal, err := ReadFromFile(p.InternalConfigFile())
	if err != nil {
		return nil, err
	}
	user, err := ReadFromFile(p.UserConfigFile())
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

func (p *PGBouncer) poolMode() (string, error) {
	conf, err := p.CurrentConfig()
	if err != nil {
		return "", err
	}

	return conf["pool_mode"].(string), nil
}

func (p *PGBouncer) initialize() error {
	cmdStr := fmt.Sprintf("mkdir -p %s", p.ConfigPath)
	if err := utils.RunCommand(cmdStr); err != nil {
		return err
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/pgbouncer.ini", p.ConfigPath), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil
	}
	defer f.Close()

	entries := []string{
		"[pgbouncer]\n",
		fmt.Sprintf("%%include %s/pgbouncer.internal.ini\n", p.ConfigPath),
		fmt.Sprintf("%%include %s/pgbouncer.user.ini\n", p.ConfigPath),
		fmt.Sprintf("%%include %s/pgbouncer.database.ini\n", p.ConfigPath),
	}

	for _, entry := range entries {
		if _, err := f.WriteString(entry); err != nil {
			return fmt.Errorf("failed append configuration entry: %s", err)
		}
	}

	p.setDefaults()

	if err := p.configureAuth(); err != nil {
		return fmt.Errorf("failed to configure pgbouncer auth. %s", err)
	}

	return nil
}

func (p *PGBouncer) setDefaults() {
	conf := ConfigMap{
		"listen_addr":          "*",
		"listen_port":          "5432",
		"auth_user":            "postgres",
		"auth_file":            fmt.Sprintf("%s/pgbouncer.auth", p.ConfigPath),
		"admin_users":          "postgres",
		"user":                 "postgres",
		"pool_mode":            "transaction",
		"min_pool_size":        "5",
		"reserve_pool_size":    "5",
		"reserve_pool_timeout": "3",
	}
	p.internalConfig = conf
}

func (p *PGBouncer) configureAuth() error {
	path := fmt.Sprintf("%s/pgbouncer.auth", p.ConfigPath)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	contents := fmt.Sprintf("\"%s\" \"%s\"", p.Credentials.Username, p.Credentials.Password)
	_, err = file.Write([]byte(contents))
	return err
}

func (p *PGBouncer) reloadConfig(ctx context.Context) error {
	conn, err := p.NewConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "RELOAD;")
	return err
}

func (p *PGBouncer) forceReconnect(ctx context.Context, databases []string) error {
	conn, err := p.NewConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, db := range databases {
		_, err = conn.Exec(ctx, fmt.Sprintf("RECONNECT %s;", db))
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PGBouncer) killConnections(ctx context.Context, databases []string) error {
	conn, err := p.NewConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, db := range databases {
		_, err = conn.Exec(ctx, fmt.Sprintf("KILL %s;", db))
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PGBouncer) resumeConnections(ctx context.Context, databases []string) error {
	conn, err := p.NewConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	for _, db := range databases {
		_, err = conn.Exec(ctx, fmt.Sprintf("RESUME %s;", db))
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PGBouncer) NewConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(p.PrivateIP, strconv.Itoa(p.Port))
	return openConnection(ctx, host, "pgbouncer", p.Credentials)
}
