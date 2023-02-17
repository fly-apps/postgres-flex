package utils

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
)

func RunCommand(cmdStr, user string) ([]byte, error) {
	pgUID, pgGID, err := SystemUserIDs(user)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(pgUID), Gid: uint32(pgGID)}
	return cmd.Output()
}

func SetFileOwnership(pathToFile, owner string) error {
	uid, gid, err := SystemUserIDs(owner)
	if err != nil {
		return fmt.Errorf("failed to resolve system user ids: %s", err)
	}

	if err := os.Chown(pathToFile, uid, gid); err != nil {
		return fmt.Errorf("failed to set ownership on file %s: %s", pathToFile, err)
	}

	return nil
}

func SystemUserIDs(usr string) (int, int, error) {
	pgUser, err := user.Lookup(usr)
	if err != nil {
		return 0, 0, err
	}
	pgUID, err := strconv.Atoi(pgUser.Uid)
	if err != nil {
		return 0, 0, err
	}
	pgGID, err := strconv.Atoi(pgUser.Gid)
	if err != nil {
		return 0, 0, err
	}

	return pgUID, pgGID, nil
}
