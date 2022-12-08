package flypg

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strings"
)

func InitializeManager(node Node) error {
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

func cloneFromPrimary(node Node, ipStr string) error {
	cmdStr := fmt.Sprintf("mkdir -p %s", node.DataDir)
	if err := runCommand(cmdStr); err != nil {
		return err
	}

	fmt.Println("Cloning from primary")
	cmdStr = fmt.Sprintf("repmgr -h %s -d %s -U %s -f %s standby clone -F",
		ipStr,
		node.ManagerDatabaseName,
		node.ManagerCredentials.Username,
		node.ManagerConfigPath)

	fmt.Println(cmdStr)
	if err := runCommand(cmdStr); err != nil {
		return err
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

func writeManagerConf(node Node) error {
	_, err := os.Stat(node.ManagerConfigPath)
	if !os.IsNotExist(err) {
		return nil
	}

	file, err := os.OpenFile(node.ManagerConfigPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	nodeName := node.PrivateIP.String()

	// This is kinda dumb, but should work.
	ipArr := strings.Split(nodeName, ":")
	lastHalf := strings.Join(ipArr[4:], "")
	seed := binary.LittleEndian.Uint32([]byte(lastHalf))
	rand.Seed(int64(seed))
	nodeID := rand.Int31()

	conf := map[string]interface{}{
		"node_id":        fmt.Sprint(nodeID),
		"node_name":      fmt.Sprintf("'%s'", nodeName),
		"conninfo":       fmt.Sprintf("'host=%s user=%s dbname=%s connect_timeout=10'", nodeName, node.ManagerCredentials.Username, node.ManagerDatabaseName),
		"data_directory": fmt.Sprintf("'%s'", node.DataDir),
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
