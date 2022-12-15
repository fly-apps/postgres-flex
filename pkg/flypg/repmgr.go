package flypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/jackc/pgx/v4"
)

const (
	primaryRoleName = "primary"
	standbyRoleName = "standby"
	unknownRoleName = ""
)

type RepMgr struct {
	ID           int32
	Region       string
	PrivateIP    string
	DataDir      string
	DatabaseName string
	Credentials  Credentials
	ConfigPath   string
	Port         int
}

func (r *RepMgr) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(r.PrivateIP, strconv.Itoa(r.Port))
	return openConnection(ctx, host, "repmgr", r.Credentials)
}

func (r *RepMgr) NewRemoteConnection(ctx context.Context, hostname string) (*pgx.Conn, error) {
	host := net.JoinHostPort(hostname, strconv.Itoa(r.Port))
	return openConnection(ctx, host, "repmgr", r.Credentials)
}

func (r *RepMgr) initialize() error {
	if err := r.writeManagerConf(); err != nil {
		return fmt.Errorf("failed to write repmgr config file: %s", err)
	}

	if err := r.writePasswdConf(); err != nil {
		return fmt.Errorf("failed creating pgpass file: %s", err)
	}

	if err := setDirOwnership(); err != nil {
		return fmt.Errorf("failed to set dir ownership: %s", err)
	}

	return nil
}

func (r *RepMgr) setup(conn *pgx.Conn) error {
	if _, err := admin.CreateDatabase(conn, r.DatabaseName, r.Credentials.Username); err != nil {
		return fmt.Errorf("failed to create repmgr database: %s", err)
	}

	if err := admin.EnableExtension(conn, "repmgr"); err != nil {
		return fmt.Errorf("failed to enable repmgr extension: %s", err)
	}

	if err := r.registerPrimary(); err != nil {
		return fmt.Errorf("failed to register repmgr primary: %s", err)
	}

	return nil
}

func (r *RepMgr) currentRole(ctx context.Context, pg *pgx.Conn) (string, error) {
	return r.memberRole(ctx, pg, int(r.ID))
}

func (r *RepMgr) writeManagerConf() error {
	file, err := os.OpenFile(r.ConfigPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	conf := map[string]interface{}{
		"node_id":                    fmt.Sprint(r.ID),
		"node_name":                  fmt.Sprintf("'%s'", r.PrivateIP),
		"conninfo":                   fmt.Sprintf("'host=%s port=%d user=%s dbname=%s connect_timeout=10'", r.PrivateIP, r.Port, r.Credentials.Username, r.DatabaseName),
		"data_directory":             fmt.Sprintf("'%s'", r.DataDir),
		"failover":                   "'automatic'",
		"promote_command":            fmt.Sprintf("'repmgr standby promote -f %s --log-to-file'", r.ConfigPath),
		"follow_command":             fmt.Sprintf("'repmgr standby follow -f %s --log-to-file --upstream-node-id=%%n'", r.ConfigPath),
		"event_notification_command": fmt.Sprintf("'/usr/local/bin/event_handler -node-id %%n -event %%e -success %%s -details \"%%d\" -new-node-id \\'%%p\\''"),
		"event_notifications":        "'repmgrd_failover_promote,standby_promote,standby_follow'",
		"location":                   r.Region,
	}

	if !r.eligiblePrimary() {
		conf["priority"] = "0"
	}

	for key, value := range conf {
		str := fmt.Sprintf("%s=%s\n", key, value)
		_, err := file.Write([]byte(str))
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RepMgr) registerPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary register -F -v", r.ConfigPath)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func (r *RepMgr) unregisterPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary unregister", r.ConfigPath)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func (r *RepMgr) followPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s standby follow", r.ConfigPath)
	if err := runCommand(cmdStr); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func (r *RepMgr) registerStandby() error {
	// Force re-registry to ensure the standby picks up any new configuration changes.
	cmdStr := fmt.Sprintf("repmgr -f %s standby register -F", r.ConfigPath)
	if err := runCommand(cmdStr); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func (r *RepMgr) clonePrimary(ipStr string) error {
	cmdStr := fmt.Sprintf("mkdir -p %s", r.DataDir)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	cmdStr = fmt.Sprintf("repmgr -h %s -p %d -d %s -U %s -f %s standby clone -F",
		ipStr,
		r.Port,
		r.DatabaseName,
		r.Credentials.Username,
		r.ConfigPath)

	fmt.Println(cmdStr)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func (r *RepMgr) writePasswdConf() error {
	path := "/data/.pgpass"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	entries := []string{
		fmt.Sprintf("*:*:*:%s:%s", r.Credentials.Username, r.Credentials.Password),
	}

	for _, entry := range entries {
		str := fmt.Sprintf("%s\n", entry)
		_, err := file.Write([]byte(str))
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RepMgr) memberRole(ctx context.Context, pg *pgx.Conn, id int) (string, error) {
	sql := fmt.Sprintf("select n.type from repmgr.nodes n LEFT JOIN repmgr.nodes un ON un.node_id = n.upstream_node_id WHERE n.node_id = '%d';", id)
	var role string
	err := pg.QueryRow(ctx, sql).Scan(&role)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return role, nil
}

func (r *RepMgr) memberRoleByHostname(ctx context.Context, pg *pgx.Conn, hostname string) (string, error) {
	sql := fmt.Sprintf("select n.type from repmgr.nodes n LEFT JOIN repmgr.nodes un ON un.node_id = n.upstream_node_id where n.connInfo LIKE '%%%s%%';", hostname)
	var role string
	err := pg.QueryRow(ctx, sql).Scan(&role)
	if err != nil {
		if err == pgx.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return role, nil
}

func (r *RepMgr) eligiblePrimary() bool {
	if r.Region == os.Getenv("PRIMARY_REGION") {
		return true
	}
	return false
}
