package flypg

import (
	"fmt"
	"os"
	"os/exec"
)

func writeSSHKey() error {
	var (
		key  = os.Getenv("SSH_KEY")
		cert = os.Getenv("SSH_CERT")
	)

	err := os.Mkdir("/data/.ssh", 0700)
	if err != nil && !os.IsExist(err) {
		return err
	}

	keyFile, err := os.Create("/data/.ssh/id_rsa")
	if err != nil {
		return err
	}
	defer keyFile.Close()
	_, err = keyFile.Write([]byte(key))
	if err != nil {
		return err
	}

	certFile, err := os.Create("/data/.ssh/id_rsa-cert.pub")
	if err != nil {
		return err
	}
	defer certFile.Close()

	_, err = certFile.Write([]byte(cert))
	if err != nil {
		return err
	}

	cmdStr := fmt.Sprintf("chmod 600 %s %s", "/data/.ssh/id_rsa", "/data/.ssh/id_rsa-cert.pub")
	cmd := exec.Command("sh", "-c", cmdStr)
	if _, err := cmd.Output(); err != nil {
		return err
	}

	return nil
}
