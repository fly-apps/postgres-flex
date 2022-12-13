package flypg

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

const (
	primaryRoleName = "primary"
	standbyRoleName = "standby"
)

func initializeRepmgr(node Node) error {
	// Write conf file.
	if err := writeManagerConf(node); err != nil {
		return fmt.Errorf("failed to write repmgr config file: %s", err)
	}

	// Write passwd file.
	if err := writePasswdConf(node); err != nil {
		return fmt.Errorf("failed creating pgpass file: %s", err)
	}

	if err := setDirOwnership(); err != nil {
		return fmt.Errorf("failed to set dir ownership: %s", err)
	}

	return nil
}

func registerPrimary(node Node) error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary register",
		node.ManagerConfigPath,
	)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func unregisterPrimary(node Node) error {
	cmdStr := fmt.Sprintf("repmgr -f %s primary unregister",
		node.ManagerConfigPath,
	)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func standbyFollow(node Node) error {
	cmdStr := fmt.Sprintf("repmgr -f %s standby follow", node.ManagerConfigPath)
	if err := runCommand(cmdStr); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func registerStandby(node Node) error {
	cmdStr := fmt.Sprintf("repmgr -f %s standby register", node.ManagerConfigPath)
	if err := runCommand(cmdStr); err != nil {
		fmt.Printf("failed to register standby: %s", err)
	}

	return nil
}

func cloneFromPrimary(node Node, ipStr string) error {
	cmdStr := fmt.Sprintf("mkdir -p %s", node.DataDir)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	cmdStr = fmt.Sprintf("repmgr -h %s -p %d -d %s -U %s -f %s standby clone -F",
		ipStr,
		node.PGPort,
		node.ManagerDatabaseName,
		node.ManagerCredentials.Username,
		node.ManagerConfigPath)

	fmt.Println(cmdStr)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	return nil
}

func writeManagerConf(node Node) error {
	file, err := os.OpenFile(node.ManagerConfigPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	conf := map[string]interface{}{
		"node_id":                    fmt.Sprint(node.ID),
		"node_name":                  fmt.Sprintf("'%s'", node.PrivateIP),
		"conninfo":                   fmt.Sprintf("'host=%s port=%d user=%s dbname=%s connect_timeout=10'", node.PrivateIP, node.PGPort, node.ManagerCredentials.Username, node.ManagerDatabaseName),
		"data_directory":             fmt.Sprintf("'%s'", node.DataDir),
		"failover":                   "'automatic'",
		"promote_command":            fmt.Sprintf("'repmgr standby promote -f %s --log-to-file'", node.ManagerConfigPath),
		"follow_command":             fmt.Sprintf("'repmgr standby follow -f %s --log-to-file --upstream-node-id=%%n'", node.ManagerConfigPath),
		"event_notification_command": fmt.Sprintf("'/usr/local/bin/event_handler -node-id %%n -event %%e -success %%s -details \"%%d\" -new-node-id \\'%%p\\''"),
		"event_notifications":        "'repmgrd_failover_promote,standby_promote,standby_follow'",
		"location":                   node.Region,
	}

	if !node.ValidPrimary() {
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

func writePasswdConf(node Node) error {
	path := "/data/.pgpass"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	entries := []string{
		fmt.Sprintf("*:*:*:%s:%s", node.ManagerCredentials.Username, node.ManagerCredentials.Password),
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

func memberRole(ctx context.Context, pg *pgx.Conn, id int) (string, error) {
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

func memberRoleByHostname(ctx context.Context, pg *pgx.Conn, hostname string) (string, error) {
	sql := fmt.Sprintf("select n.type from repmgr.nodes n LEFT JOIN repmgr.nodes un ON un.node_id = n.upstream_node_id where n.connInfo LIKE '%%%s';", hostname)
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
