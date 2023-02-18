package flypg

import (
	"fmt"
	"os"
	"os/exec"
)

const (
	privateKeyFile = "/data/.ssh/id_rsa"
	publicKeyFile  = "/data/.ssh/id_rsa-cert.pub"
)

func writeSSHKey() error {
	err := os.Mkdir("/data/.ssh", 0700)
	if err != nil && !os.IsExist(err) {
		return err
	}

	if err := writePrivateKey(); err != nil {
		return fmt.Errorf("failed to write private key: %s", err)
	}

	if err := writePublicKey(); err != nil {
		return fmt.Errorf("failed to write cert: %s", err)
	}

	cmdStr := fmt.Sprintf("chmod 600 %s %s", privateKeyFile, publicKeyFile)
	cmd := exec.Command("sh", "-c", cmdStr)
	if _, err := cmd.Output(); err != nil {
		return err
	}

	return nil
}

func writePrivateKey() error {
	key := os.Getenv("SSH_KEY")

	file, err := os.Create(privateKeyFile)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	_, err = file.Write([]byte(key))
	if err != nil {
		return fmt.Errorf("failed to write contents to pvt key: %s", err)
	}

	return file.Sync()
}

func writePublicKey() error {
	cert := os.Getenv("SSH_CERT")

	file, err := os.Create(publicKeyFile)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	_, err = file.Write([]byte(cert))
	if err != nil {
		return fmt.Errorf("failed to write contents to pub key: %s", err)
	}

	return file.Sync()
}
