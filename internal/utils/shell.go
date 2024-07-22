package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
)

func RunCmd(ctx context.Context, usr string, name string, args ...string) ([]byte, error) {
	uid, gid, err := SystemUserIDs(usr)
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}

	if os.Getenv("DEBUG") != "" {
		log.Printf("> Running command as %s: %s\n", usr, cmd.String())

		var stdoutBuf, stderrBuf bytes.Buffer
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

		err = cmd.Run()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				ee.Stderr = stderrBuf.Bytes()
			}
		}

		return stdoutBuf.Bytes(), err
	}

	return cmd.Output()
}

// Deprecated, use RunCmd instead
func RunCommand(cmdStr, usr string) ([]byte, error) {
	uid, gid, err := SystemUserIDs(usr)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("sh", "-c", cmdStr)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}

	if os.Getenv("DEBUG") != "" {
		log.Printf("> Running command as %s: %s\n", usr, cmdStr)

		var stdoutBuf, stderrBuf bytes.Buffer
		cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

		err = cmd.Run()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				ee.Stderr = stderrBuf.Bytes()
			}
		}

		return stdoutBuf.Bytes(), err
	}

	return cmd.Output()
}

func SetFileOwnership(pathToFile, owner string) error {
	if os.Getenv("UNIT_TESTING") != "" {
		return nil
	}

	uid, gid, err := SystemUserIDs(owner)
	if err != nil {
		return fmt.Errorf("failed to resolve system user ids: %s", err)
	}

	if err := os.Chown(pathToFile, uid, gid); err != nil {
		return fmt.Errorf("failed to set ownership on file %s: %s", pathToFile, err)
	}

	return nil
}

func FileExists(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
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
