package flypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/fly-apps/postgres-flex/pkg/utils"

	"github.com/fly-apps/postgres-flex/pkg/flypg/admin"
	"github.com/jackc/pgx/v4"
)

const (
	PrimaryRoleName = "primary"
	StandbyRoleName = "standby"
	UnknownRoleName = ""
)

type RepMgr struct {
	ID                 int32
	Region             string
	PrivateIP          string
	DataDir            string
	DatabaseName       string
	Credentials        Credentials
	ConfigPath         string
	UserConfigPath     string
	InternalConfigPath string
	Port               int

	internalConfig ConfigMap
	userConfig     ConfigMap
}

func (r *RepMgr) InternalConfigFile() string {
	return r.InternalConfigPath
}

func (r *RepMgr) UserConfigFile() string {
	return r.UserConfigPath
}

func (r *RepMgr) InternalConfig() ConfigMap {
	return r.internalConfig
}

func (r *RepMgr) UserConfig() ConfigMap {
	return r.userConfig
}

func (r *RepMgr) SetUserConfig(configMap ConfigMap) {
	r.userConfig = configMap
}

func (r *RepMgr) ConsulKey() string {
	return "repmgr"
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
	r.setDefaults()

	f, err := os.OpenFile(r.ConfigPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil
	}
	defer f.Close()

	entries := []string{"include 'repmgr.internal.conf'\n", "include 'repmgr.user.conf'\n"}

	for _, entry := range entries {
		if _, err := f.WriteString(entry); err != nil {
			return fmt.Errorf("failed append configuration entry: %s", err)
		}
	}

	if err := r.writePasswdConf(); err != nil {
		return fmt.Errorf("failed creating pgpass file: %s", err)
	}

	if err := setDirOwnership(); err != nil {
		return fmt.Errorf("failed to set dir ownership: %s", err)
	}

	return nil
}

func (r *RepMgr) setup(ctx context.Context, conn *pgx.Conn) error {
	if err := admin.CreateDatabaseWithOwner(ctx, conn, r.DatabaseName, r.Credentials.Username); err != nil {
		return fmt.Errorf("failed to create repmgr database: %s", err)
	}

	if err := admin.EnableExtension(ctx, conn, "repmgr"); err != nil {
		return fmt.Errorf("failed to enable repmgr extension: %s", err)
	}

	if err := r.registerPrimary(); err != nil {
		return fmt.Errorf("failed to register repmgr primary: %s", err)
	}

	return nil
}

func (r *RepMgr) CurrentRole(ctx context.Context, pg *pgx.Conn) (string, error) {
	return r.memberRole(ctx, pg, int(r.ID))
}

func (r *RepMgr) Standbys(ctx context.Context, pg *pgx.Conn) ([]Standby, error) {
	return r.standbyStatuses(ctx, pg, int(r.ID))
}

func (r *RepMgr) setDefaults() {
	conf := ConfigMap{
		"node_id":                      fmt.Sprint(r.ID),
		"node_name":                    fmt.Sprintf("'%s'", r.PrivateIP),
		"conninfo":                     fmt.Sprintf("'host=%s port=%d user=%s dbname=%s connect_timeout=10'", r.PrivateIP, r.Port, r.Credentials.Username, r.DatabaseName),
		"data_directory":               fmt.Sprintf("'%s'", r.DataDir),
		"failover":                     "'automatic'",
		"use_replication_slots":        "yes",
		"promote_command":              fmt.Sprintf("'repmgr standby promote -f %s --log-to-file'", r.ConfigPath),
		"follow_command":               fmt.Sprintf("'repmgr standby follow -f %s --log-to-file --upstream-node-id=%%n'", r.ConfigPath),
		"event_notification_command":   fmt.Sprintf("'/usr/local/bin/event_handler -node-id %%n -event %%e -success %%s -details \"%%d\" -new-node-id \\'%%p\\''"),
		"event_notifications":          "'repmgrd_failover_promote,standby_promote,standby_follow'",
		"location":                     r.Region,
		"primary_visibility_consensus": true,
		"failover_validation_command":  fmt.Sprintf("'/usr/local/bin/failover_validation -node-id %%n -node-name %%a -visable-nodes %%v -total-nodes %%t'"),
	}

	if !r.eligiblePrimary() {
		conf["priority"] = "0"
	}

	r.internalConfig = conf
}

func (r *RepMgr) registerPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary register -F -v", r.ConfigPath)
	if err := utils.RunCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func (r *RepMgr) unregisterPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary unregister", r.ConfigPath)
	if err := utils.RunCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func (r *RepMgr) followPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s standby follow", r.ConfigPath)
	if err := utils.RunCommand(cmdStr); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func (r *RepMgr) registerStandby() error {
	// Force re-registry to ensure the standby picks up any new configuration changes.
	cmdStr := fmt.Sprintf("repmgr -f %s standby register -F", r.ConfigPath)
	if err := utils.RunCommand(cmdStr); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func (r *RepMgr) UnregisterStandby(id int) error {
	cmdStr := fmt.Sprintf("repmgr standby unregister -f %s --node-id=%d", r.ConfigPath, id)
	if err := utils.RunCommand(cmdStr); err != nil {
		fmt.Printf("failed to unregister standby: %s", err)
	}

	return nil
}

func (r *RepMgr) clonePrimary(ipStr string) error {
	cmdStr := fmt.Sprintf("mkdir -p %s", r.DataDir)
	if err := utils.RunCommand(cmdStr); err != nil {
		return err
	}

	cmdStr = fmt.Sprintf("repmgr -h %s -p %d -d %s -U %s -f %s standby clone -F",
		ipStr,
		r.Port,
		r.DatabaseName,
		r.Credentials.Username,
		r.ConfigPath)

	fmt.Println(cmdStr)
	return utils.RunCommand(cmdStr)
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

type Standby struct {
	Id int
	Ip string
}

func (r *RepMgr) standbyStatuses(ctx context.Context, pg *pgx.Conn, id int) ([]Standby, error) {
	sql := fmt.Sprintf("select node_id, node_name from repmgr.show_nodes where type = 'standby' and upstream_node_id = '%d';", id)
	var standbys []Standby
	rows, err := pg.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var s Standby
		err := rows.Scan(&s.Id, &s.Ip)
		if err != nil {
			return nil, err
		}
		standbys = append(standbys, s)
	}
	return standbys, nil
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
	return r.Region == os.Getenv("PRIMARY_REGION")
}
