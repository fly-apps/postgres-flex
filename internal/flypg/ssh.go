package flypg

import (
	"fmt"
	"os"
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

	if err := os.Chmod(privateKeyFile, 0600); err != nil {
		return fmt.Errorf("failed to chmod private key file: %s", err)
	}

	if err := os.Chmod(publicKeyFile, 0600); err != nil {
		return fmt.Errorf("failed to chmod public key file: %s", err)
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
