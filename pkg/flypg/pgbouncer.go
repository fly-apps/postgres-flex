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

func (p *PGBouncer) NewConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(p.PrivateIP, strconv.Itoa(p.Port))
	return openConnection(ctx, host, "pgbouncer", p.Credentials)
}

func (p *PGBouncer) ConfigurePrimary(primary string, reload bool) error {
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
		err = p.reloadConfig()
		if err != nil {
			fmt.Printf("failed to reconfigure pgbouncer primary %s\n", err)
		}
	}
	return nil
}

func (p *PGBouncer) configure(primary string) error {
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

	p.configureAuth()
	p.ConfigurePrimary(primary, false)

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
	if err != nil {
		return err
	}
	return nil
}

func (p *PGBouncer) reloadConfig() error {
	conn, err := p.NewConnection(context.TODO())
	if err != nil {
		return err
	}
	_, err = conn.Exec(context.TODO(), "RELOAD;")
	if err != nil {
		return err
	}
	return nil
}
