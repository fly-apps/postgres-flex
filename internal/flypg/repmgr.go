package flypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/fly-apps/postgres-flex/internal/privnet"
	"github.com/fly-apps/postgres-flex/internal/utils"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/jackc/pgx/v5"
)

const (
	PrimaryRoleName = "primary"
	StandbyRoleName = "standby"
	UnknownRoleName = ""
)

type RepMgr struct {
	ID                 int32
	AppName            string
	PrimaryRegion      string
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

func (r *RepMgr) CurrentConfig() (ConfigMap, error) {
	internal, err := ReadFromFile(r.InternalConfigFile())
	if err != nil {
		return nil, err
	}
	user, err := ReadFromFile(r.UserConfigFile())
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

func (*RepMgr) ConsulKey() string {
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

	file, err := os.Create(r.ConfigPath)
	if err != nil {
		return nil
	}
	defer func() { _ = file.Close() }()

	entries := []string{"include 'repmgr.internal.conf'\n", "include 'repmgr.user.conf'\n"}
	for _, entry := range entries {
		if _, err := file.WriteString(entry); err != nil {
			return fmt.Errorf("failed append configuration entry: %s", err)
		}
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %s", err)
	} else if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %s", err)
	}

	if err := r.writePasswdConf(); err != nil {
		return fmt.Errorf("failed creating pgpass file: %s", err)
	}

	if err := utils.SetFileOwnership(r.ConfigPath, "postgres"); err != nil {
		return fmt.Errorf("failed to set repmgr.conf ownership: %s", err)
	}

	return file.Sync()
}

func (r *RepMgr) setup(ctx context.Context, conn *pgx.Conn) error {
	if err := admin.CreateDatabaseWithOwner(ctx, conn, r.DatabaseName, r.Credentials.Username); err != nil {
		return fmt.Errorf("failed to create repmgr database: %s", err)
	}

	if err := admin.EnableExtension(ctx, conn, "repmgr"); err != nil {
		return fmt.Errorf("failed to enable repmgr extension: %s", err)
	}

	return nil
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
		"event_notification_command":   fmt.Sprintf("'/usr/local/bin/event_handler -node-id %%n -event %%e -success %%s -details \"%%d\"'"),
		"event_notifications":          "'child_node_disconnect,child_node_reconnect,child_node_new_connect'",
		"location":                     r.Region,
		"primary_visibility_consensus": true,
		"failover_validation_command":  fmt.Sprintf("'/usr/local/bin/failover_validation -visible-nodes %%v -total-nodes %%t'"),
		"ssh_options":                  "'-o \"StrictHostKeyChecking=no\"'",
		"priority":                     100,
	}

	if !r.eligiblePrimary() {
		conf["priority"] = "0"
	}

	r.internalConfig = conf
}

func (r *RepMgr) registerPrimary() error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary register -F -v", r.ConfigPath)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (r *RepMgr) unregisterPrimary(id int) error {
	cmdStr := fmt.Sprintf("repmgr primary unregister -f %s --node-id=%d", r.ConfigPath, id)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (r *RepMgr) rejoinCluster(hostname string) error {
	cmdStr := fmt.Sprintf("repmgr -f %s node rejoin -h %s -p %d -U %s -d %s --force-rewind --no-wait",
		r.ConfigPath,
		hostname,
		r.Port,
		r.Credentials.Username,
		r.DatabaseName,
	)

	fmt.Println(cmdStr)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (r *RepMgr) registerStandby() error {
	// Force re-registry to ensure the standby picks up any new configuration changes.
	cmdStr := fmt.Sprintf("repmgr -f %s standby register -F", r.ConfigPath)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func (r *RepMgr) unregisterStandby(id int) error {
	cmdStr := fmt.Sprintf("repmgr standby unregister -f %s --node-id=%d", r.ConfigPath, id)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		fmt.Printf("failed to unregister standby: %s", err)
	}

	return nil
}

func (r *RepMgr) clonePrimary(ipStr string) error {
	cmdStr := fmt.Sprintf("mkdir -p %s", r.DataDir)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to create pg directory: %s", err)
	}

	cmdStr = fmt.Sprintf("repmgr -h %s -p %d -d %s -U %s -f %s standby clone -F",
		ipStr,
		r.Port,
		r.DatabaseName,
		r.Credentials.Username,
		r.ConfigPath)

	fmt.Println(cmdStr)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to clone primary: %s", err)
	}

	return nil
}

func (r *RepMgr) writePasswdConf() error {
	path := "/data/.pgpass"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open repmgr password file: %s", err)
	}
	defer file.Close()

	if err := utils.SetFileOwnership(path, "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership: %s", err)
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

	return file.Sync()
}

type Member struct {
	ID       int
	Hostname string
	Active   bool
	Region   string
	Role     string
}

func (*RepMgr) Members(ctx context.Context, pg *pgx.Conn) ([]Member, error) {
	sql := "select node_id, node_name, location, active, type from repmgr.nodes;"
	rows, err := pg.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []Member
	for rows.Next() {
		var member Member
		if err := rows.Scan(&member.ID, &member.Hostname, &member.Region, &member.Active, &member.Role); err != nil {
			return nil, err
		}

		members = append(members, member)
	}

	return members, nil
}

func (r *RepMgr) Member(ctx context.Context, conn *pgx.Conn) (*Member, error) {
	members, err := r.Members(ctx, conn)
	if err != nil {
		return nil, err
	}

	for _, member := range members {
		if member.Hostname == r.PrivateIP {
			return &member, nil
		}
	}

	return nil, pgx.ErrNoRows
}

func (*RepMgr) PrimaryMember(ctx context.Context, pg *pgx.Conn) (*Member, error) {
	var member Member
	sql := "select node_id, node_name, location, active, type from repmgr.nodes where type = 'primary' and active = true;"
	err := pg.QueryRow(ctx, sql).Scan(&member.ID, &member.Hostname, &member.Region, &member.Active, &member.Role)
	if err != nil {
		return nil, err
	}

	return &member, nil
}

func (r *RepMgr) StandbyMembers(ctx context.Context, conn *pgx.Conn) ([]Member, error) {
	members, err := r.Members(ctx, conn)
	if err != nil {
		return nil, err
	}

	var standbys []Member
	for _, member := range members {
		if member.Role == StandbyRoleName {
			standbys = append(standbys, member)
		}
	}

	return standbys, nil
}

func (*RepMgr) MemberByID(ctx context.Context, pg *pgx.Conn, id int) (*Member, error) {
	var member Member
	sql := fmt.Sprintf("select node_id, node_name, location, active, type from repmgr.nodes where node_id = %d;", id)

	err := pg.QueryRow(ctx, sql).Scan(&member.ID, &member.Hostname, &member.Region, &member.Active, &member.Role)
	if err != nil {
		return nil, err
	}

	return &member, nil
}

func (*RepMgr) MemberByHostname(ctx context.Context, pg *pgx.Conn, hostname string) (*Member, error) {
	var member Member
	sql := fmt.Sprintf("select node_id, node_name, location, active, type from repmgr.nodes where node_name = '%s';", hostname)

	err := pg.QueryRow(ctx, sql).Scan(&member.ID, &member.Hostname, &member.Region, &member.Active, &member.Role)
	if err != nil {
		return nil, err
	}

	return &member, nil
}

func (r *RepMgr) ResolveMemberOverDNS(ctx context.Context) (*Member, error) {
	ips, err := r.InRegionPeerIPs(ctx)
	if err != nil {
		return nil, err
	}

	var target *Member

	for _, ip := range ips {
		if ip.String() == r.PrivateIP {
			continue
		}

		conn, err := r.NewRemoteConnection(ctx, ip.String())
		if err != nil {
			fmt.Printf("failed to connect to %s", ip.String())
			continue
		}
		defer conn.Close(ctx)

		member, err := r.MemberByHostname(ctx, conn, ip.String())
		if err != nil {
			fmt.Printf("failed to resolve role from %s", ip.String())
			continue
		}

		if member.Role == PrimaryRoleName || member.Role == StandbyRoleName {
			target = member
			break
		}
	}

	if target == nil {
		return nil, fmt.Errorf("unable to resolve cloneable member")
	}

	return target, nil
}

func (r *RepMgr) InRegionPeerIPs(ctx context.Context) ([]net.IPAddr, error) {
	targets := fmt.Sprintf("%s.%s", r.PrimaryRegion, r.AppName)
	return privnet.AllPeers(ctx, targets)
}

func (r *RepMgr) HostInRegion(ctx context.Context, hostname string) (bool, error) {
	ips, err := r.InRegionPeerIPs(ctx)
	if err != nil {
		return false, err
	}

	for _, ip := range ips {
		if ip.String() == hostname {
			return true, nil
		}
	}

	return false, nil
}

func (r *RepMgr) UnregisterMember(member Member) error {
	if member.Role == PrimaryRoleName {
		if err := r.unregisterPrimary(member.ID); err != nil {
			return fmt.Errorf("failed to unregister member %d: %s", member.ID, err)
		}
		return nil
	}

	if err := r.unregisterStandby(member.ID); err != nil {
		return fmt.Errorf("failed to unregister member %d: %s", member.ID, err)
	}

	return nil
}

func (r *RepMgr) eligiblePrimary() bool {
	return r.Region == r.PrimaryRegion
}
