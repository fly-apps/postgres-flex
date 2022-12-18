package flypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/jackc/pgx/v4"
)

type PGBouncer struct {
	PrivateIP   string
	Credentials Credentials
	ConfigPath  string
	Port        int
	ForwardPort int
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
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	}
	return nil
}

func (p *PGBouncer) initialize() error {
	cmdStr := fmt.Sprintf("mkdir -p %s", p.ConfigPath)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	// If pgbouncer.ini file is not present, set defaults.
	if _, err := os.Stat(fmt.Sprintf("%s/pgbouncer.ini", p.ConfigPath)); err != nil {
		if os.IsNotExist(err) {
			cmdStr := fmt.Sprintf("cp /fly/pgbouncer.ini %s", p.ConfigPath)
			if err := runCommand(cmdStr); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if err := p.configureAuth(); err != nil {
		return fmt.Errorf("failed to configure pgbouncer auth. %s", err)
	}

	return nil
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
	conn, err := p.newConnection(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "RELOAD;")
	return err
}

func (p *PGBouncer) newConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(p.PrivateIP, strconv.Itoa(p.Port))
	return openConnection(ctx, host, "pgbouncer", p.Credentials)
}
