//go:build !linux
// +build !linux

package supervisor

import "os/exec"

func ensureKill(cmd *exec.Cmd) {
	// cmd.SysProcAttr.Pdeathsig is supported on Linux, we can't do anything here
}
