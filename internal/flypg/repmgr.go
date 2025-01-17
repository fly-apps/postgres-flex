package flypg

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/fly-apps/postgres-flex/internal/privnet"
	"github.com/fly-apps/postgres-flex/internal/utils"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/jackc/pgx/v5"
)

const (
	PrimaryRoleName = "primary"
	StandbyRoleName = "standby"
	WitnessRoleName = "witness"
	UnknownRoleName = ""

	repmgrConsulKey = "repmgr"
)

type RepMgr struct {
	ID                 int32
	AppName            string
	PrimaryRegion      string
	Region             string
	HostName           string
	PrivateIP          string
	MachineID          string
	DataDir            string
	DatabaseName       string
	Credentials        admin.Credential
	ConfigPath         string
	UserConfigPath     string
	PasswordConfigPath string
	InternalConfigPath string
	Port               int
	Witness            bool

	internalConfig ConfigMap
	userConfig     ConfigMap
}

func (*RepMgr) ConsulKey() string {
	return repmgrConsulKey
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

func (r *RepMgr) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(r.PrivateIP, strconv.Itoa(r.Port))
	return openConnection(ctx, host, r.DatabaseName, r.Credentials)
}

func (r *RepMgr) NewRemoteConnection(ctx context.Context, hostname string) (*pgx.Conn, error) {
	host := net.JoinHostPort(hostname, strconv.Itoa(r.Port))
	return openConnection(ctx, host, r.DatabaseName, r.Credentials)
}

func (r *RepMgr) initialize() error {
	entries := []string{
		"include 'repmgr.internal.conf'\n",
		"include 'repmgr.user.conf'\n",
	}

	entriesStr := strings.Join(entries, "")
	if err := os.WriteFile(r.ConfigPath, []byte(entriesStr), 0600); err != nil {
		return fmt.Errorf("failed to create %s: %s", r.ConfigPath, err)
	}

	if err := utils.SetFileOwnership(r.ConfigPath, "postgres"); err != nil {
		return fmt.Errorf("failed to set repmgr.conf ownership: %s", err)
	}

	// Create password file that repmgr will hook into for internal operations.
	passStr := fmt.Sprintf("*:*:*:%s:%s", r.Credentials.Username, r.Credentials.Password)
	if err := os.WriteFile(r.PasswordConfigPath, []byte(passStr), 0600); err != nil {
		return fmt.Errorf("failed to write file %s: %s", r.PasswordConfigPath, err)
	}

	if err := utils.SetFileOwnership(r.PasswordConfigPath, "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership: %s", err)
	}

	if err := r.setDefaults(); err != nil {
		return fmt.Errorf("failed to set defaults: %s", err)
	}

	// Note - Sync from consul has been disabled for this component.
	// It will be re-enabled once we offer user-defined configuration.

	if err := WriteConfigFiles(r); err != nil {
		return fmt.Errorf("failed to write config files for repmgr: %s", err)
	}

	return nil
}

func (r *RepMgr) enable(ctx context.Context, conn *pgx.Conn) error {
	if err := admin.CreateDatabaseWithOwner(ctx, conn, r.DatabaseName, r.Credentials.Username); err != nil {
		return fmt.Errorf("failed to create repmgr database: %s", err)
	}

	if err := admin.EnableExtension(ctx, conn, r.DatabaseName); err != nil {
		return fmt.Errorf("failed to enable repmgr extension: %s", err)
	}

	return nil
}

func (r *RepMgr) setDefaults() error {
	nodeID, err := r.resolveNodeID()
	if err != nil {
		return err
	}

	conf := ConfigMap{
		"node_id":                      nodeID,
		"node_name":                    fmt.Sprintf("'%s'", r.MachineID),
		"conninfo":                     fmt.Sprintf("'host=%s port=%d user=%s dbname=%s connect_timeout=5'", r.HostName, r.Port, r.Credentials.Username, r.DatabaseName),
		"data_directory":               fmt.Sprintf("'%s'", r.DataDir),
		"failover":                     "'automatic'",
		"use_replication_slots":        "yes",
		"promote_command":              fmt.Sprintf("'repmgr standby promote -f %s --log-to-file'", r.ConfigPath),
		"follow_command":               fmt.Sprintf("'repmgr standby follow -f %s --log-to-file --upstream-node-id=%%n'", r.ConfigPath),
		"event_notification_command":   fmt.Sprintf("'/usr/local/bin/event_handler -node-id %%n -event %%e -success %%s -details \"%%d\"'"),
		"event_notifications":          "'child_node_disconnect,child_node_reconnect,child_node_new_connect'",
		"location":                     fmt.Sprintf("'%s'", r.Region),
		"primary_visibility_consensus": true,
		"failover_validation_command":  fmt.Sprintf("'/usr/local/bin/failover_validation -visible-nodes %%v -total-nodes %%t'"),
		"ssh_options":                  "'-o \"StrictHostKeyChecking=no\"'",
		"priority":                     100,
		"node_rejoin_timeout":          30,
		"standby_reconnect_timeout":    30,
		"passfile":                     fmt.Sprintf("'%s'", r.PasswordConfigPath),
	}

	if !r.eligiblePrimary() {
		conf["priority"] = "0"
	}

	r.internalConfig = conf

	return nil
}

func (r *RepMgr) resolveNodeID() (string, error) {
	var nodeID string
	if utils.FileExists(r.InternalConfigFile()) {
		// Pull existing id from configuraiton file
		config, err := r.CurrentConfig()
		if err != nil {
			return "", fmt.Errorf("failed to resolve current repmgr config: %s", err)
		}

		if val, ok := config["node_id"]; ok {
			nodeID = fmt.Sprint(val)
		}

		if nodeID == "" {
			return "", fmt.Errorf("failed to resolve existing node_id: %s", err)
		}
	} else {
		// Generate a new random id
		id, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt32))
		if err != nil {
			return "", fmt.Errorf("failed to generate node id: %s", err)
		}
		nodeID = id.String()
	}

	return nodeID, nil
}

func (r *RepMgr) registerPrimary(restartDaemon bool) error {
	cmdStr := fmt.Sprintf("repmgr primary register -f %s -F", r.ConfigPath)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to register primary: %s", err)
	}

	if restartDaemon {
		if err := r.restartDaemon(); err != nil {
			return fmt.Errorf("failed to restart repmgr daemon: %s", err)
		}
	}

	return nil
}

func (r *RepMgr) registerStandby(restartDaemon bool) error {
	cmdStr := fmt.Sprintf("repmgr standby register -f %s -F", r.ConfigPath)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to register standby: %s", err)
	}

	if restartDaemon {
		if err := r.restartDaemon(); err != nil {
			return fmt.Errorf("failed to restart repmgr daemon: %s", err)
		}
	}

	return nil
}

func (r *RepMgr) registerWitness(primaryHostname string) error {
	cmdStr := fmt.Sprintf("repmgr witness register -f %s -h %s -F", r.ConfigPath, primaryHostname)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (r *RepMgr) unregisterPrimary(id int) error {
	cmdStr := fmt.Sprintf("repmgr primary unregister -f %s --node-id=%d", r.ConfigPath, id)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (r *RepMgr) unregisterStandby(id int) error {
	cmdStr := fmt.Sprintf("repmgr standby unregister -f %s --node-id=%d", r.ConfigPath, id)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (*RepMgr) restartDaemon() error {
	_, err := utils.RunCommand("restart-repmgrd", "postgres")
	return err
}

func (r *RepMgr) daemonRestartRequired(m *Member) bool {
	return m.Name != r.MachineID
}

func (r *RepMgr) unregisterWitness(id int) error {
	cmdStr := fmt.Sprintf("repmgr witness unregister -f %s --node-id=%d", r.ConfigPath, id)
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

	log.Println(cmdStr)
	_, err := utils.RunCommand(cmdStr, "postgres")

	return err
}

func (r *RepMgr) clonePrimary(hostname string) error {
	cmdStr := fmt.Sprintf("mkdir -p %s", r.DataDir)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to create pg directory: %s", err)
	}

	cmdStr = fmt.Sprintf("repmgr -h %s -p %d -d %s -U %s -f %s standby clone -c -F",
		hostname,
		r.Port,
		r.DatabaseName,
		r.Credentials.Username,
		r.ConfigPath)

	log.Println(cmdStr)
	if _, err := utils.RunCommand(cmdStr, "postgres"); err != nil {
		return fmt.Errorf("failed to clone primary: %s", err)
	}

	return nil
}

func (r *RepMgr) regenReplicationConf(ctx context.Context) error {
	if _, err := utils.RunCmd(ctx, "postgres",
		"repmgr", "--replication-conf-only",
		"-h", r.HostName,
		"-p", fmt.Sprint(r.Port),
		"-d", r.DatabaseName,
		"-U", r.Credentials.Username,
		"-f", r.ConfigPath,
		"standby", "clone", "-F"); err != nil {
		return fmt.Errorf("failed to regenerate replication conf: %s", err)
	}
	return nil
}

type Member struct {
	ID       int
	Name     string
	Hostname string
	Active   bool
	Region   string
	Role     string
}

func (r *RepMgr) Members(ctx context.Context, pg *pgx.Conn) ([]Member, error) {
	sql := "select node_id, node_name, location, active, type from repmgr.nodes;"
	rows, err := pg.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []Member
	for rows.Next() {
		var member Member
		if err := rows.Scan(&member.ID, &member.Name, &member.Region, &member.Active, &member.Role); err != nil {
			return nil, err
		}

		// Assume we are working with a machineID if the name is 14 characters long.
		if len(member.Name) == 14 {
			member.Hostname = r.machineIDToDNS(member.Name)
		} else {
			// Member name is the private IP.
			member.Hostname = member.Name
			member.Name = ""
		}

		members = append(members, member)
	}

	return members, nil
}

func (r *RepMgr) Member(ctx context.Context, conn *pgx.Conn) (*Member, error) {
	myID, err := r.resolveNodeID()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve node id: %s", err)
	}

	members, err := r.Members(ctx, conn)
	if err != nil {
		return nil, err
	}

	for _, member := range members {
		if fmt.Sprint(member.ID) == myID {
			return &member, nil
		}
	}

	return nil, pgx.ErrNoRows
}

func (r *RepMgr) PrimaryMember(ctx context.Context, pg *pgx.Conn) (*Member, error) {
	var member Member
	sql := "select node_id, node_name, location, active, type from repmgr.nodes where type = 'primary' and active = true;"
	err := pg.QueryRow(ctx, sql).Scan(&member.ID, &member.Name, &member.Region, &member.Active, &member.Role)
	if err != nil {
		return nil, err
	}

	// Assume we are working with a machineID if the name is 14 characters long.
	if len(member.Name) == 14 {
		member.Hostname = r.machineIDToDNS(member.Name)
	} else {
		// Member name is the private IP.
		member.Hostname = member.Name
		member.Name = ""
	}

	return &member, nil
}

func (r *RepMgr) IsPrimary(ctx context.Context, pg *pgx.Conn) (bool, error) {
	member, err := r.Member(ctx, pg)
	if err != nil {
		return false, err
	}

	return member.Role == PrimaryRoleName, nil
}

func (r *RepMgr) VotingMembers(ctx context.Context, conn *pgx.Conn) ([]Member, error) {
	members, err := r.Members(ctx, conn)
	if err != nil {
		return nil, err
	}

	var voters []Member
	for _, member := range members {
		if (member.Role == StandbyRoleName || member.Role == WitnessRoleName) && member.Region == r.PrimaryRegion {
			voters = append(voters, member)
		}
	}

	return voters, nil
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

func (r *RepMgr) MemberByNodeName(ctx context.Context, pg *pgx.Conn, name string) (*Member, error) {
	var member Member
	sql := fmt.Sprintf("select node_id, node_name, location, active, type from repmgr.nodes where node_name = '%s';", name)

	err := pg.QueryRow(ctx, sql).Scan(&member.ID, &member.Name, &member.Region, &member.Active, &member.Role)
	if err != nil {
		return nil, err
	}

	member.Hostname = r.machineIDToDNS(name)

	return &member, nil
}

func (r *RepMgr) ResolvePrimaryOverDNS(ctx context.Context) (*Member, error) {
	machineIDs, err := r.InRegionPeerMachines(ctx)
	if err != nil {
		return nil, err
	}

	var target *Member

	for _, machineID := range machineIDs {
		if machineID == r.MachineID {
			continue
		}

		hostname := r.machineIDToDNS(machineID)

		conn, err := r.NewRemoteConnection(ctx, hostname)
		if err != nil {
			continue
		}
		defer func() { _ = conn.Close(ctx) }()

		member, err := r.MemberByNodeName(ctx, conn, machineID)
		if err != nil {
			continue
		}

		if err := conn.Close(ctx); err != nil {
			return nil, fmt.Errorf("failed to close connection: %s", err)
		}

		if member.Role == PrimaryRoleName && member.Active {
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

func (r *RepMgr) InRegionPeerMachines(ctx context.Context) ([]string, error) {
	machines, err := privnet.AllMachines(ctx, r.AppName)
	if err != nil {
		return nil, err
	}

	var machineIDs []string
	for _, machine := range machines {
		if machine.Region == r.PrimaryRegion {
			machineIDs = append(machineIDs, machine.Id)
		}
	}
	return machineIDs, nil
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
	switch member.Role {
	case PrimaryRoleName:
		if err := r.unregisterPrimary(member.ID); err != nil {
			return fmt.Errorf("failed to unregister member %d: %s", member.ID, err)
		}
	case StandbyRoleName:
		if err := r.unregisterStandby(member.ID); err != nil {
			return fmt.Errorf("failed to unregister standby %d: %s", member.ID, err)
		}
	case WitnessRoleName:
		if err := r.unregisterWitness(member.ID); err != nil {
			return fmt.Errorf("failed to unregister witness %d: %s", member.ID, err)
		}
	}

	return nil
}

func (r *RepMgr) eligiblePrimary() bool {
	return r.Region == r.PrimaryRegion
}

func (r *RepMgr) machineIDToDNS(nodeName string) string {
	if len(nodeName) != 14 {
		panic("invalid machine id")
	}

	return fmt.Sprintf("%s.vm.%s.internal", nodeName, r.AppName)
}
