package utils

import (
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
)

func RunCommand(cmdStr string) error {
	pgUser, err := user.Lookup("postgres")
	if err != nil {
		return err
	}
	pgUID, err := strconv.Atoi(pgUser.Uid)
	if err != nil {
		return err
	}
	pgGID, err := strconv.Atoi(pgUser.Gid)
	if err != nil {
		return err
	}

	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(pgUID), Gid: uint32(pgGID)}
	_, err = cmd.Output()
	return err
}
