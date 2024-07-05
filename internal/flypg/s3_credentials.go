package flypg

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
)

const (
	s3AuthDir      = "/data/.aws"
	s3AuthFileName = "credentials"
)

type s3Credentials struct {
	profile         string
	accessKeyID     string
	secretAccessKey string
}

func writeS3Credentials(ctx context.Context, s3AuthDir string) error {
	var creds []*s3Credentials

	if os.Getenv("S3_ARCHIVE_CONFIG") != "" {
		cred, err := parseCredentialsFromConfigURL(os.Getenv("S3_ARCHIVE_CONFIG"), DefaultAuthProfile)
		if err != nil {
			return fmt.Errorf("failed to parse credentials from barman configURL: %v", err)
		}
		creds = append(creds, cred)
	}

	if os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG") != "" {
		cred, err := parseCredentialsFromConfigURL(os.Getenv("S3_ARCHIVE_REMOTE_RESTORE_CONFIG"), RestoreAuthProfile)
		if err != nil {
			return fmt.Errorf("failed to parse credentials from barman restore configURL: %v", err)
		}
		creds = append(creds, cred)
	}

	if len(creds) == 0 {
		return nil
	}

	s3AuthFilePath := filepath.Join(s3AuthDir, s3AuthFileName)

	// Ensure the directory exists
	if err := os.MkdirAll(s3AuthDir, 0700); err != nil {
		return fmt.Errorf("failed to create AWS credentials directory: %w", err)
	}

	// Write the credentials to disk
	if err := writeCredentialsToFile(creds, s3AuthFilePath); err != nil {
		return fmt.Errorf("failed to write credentials to file: %w", err)
	}

	// Set file permissions
	if err := os.Chmod(s3AuthFilePath, 0644); err != nil {
		return fmt.Errorf("failed to set file permissions: %w", err)
	}

	// Ensure the directory has the correct ownership
	if err := setDirOwnership(ctx, s3AuthDir); err != nil {
		return fmt.Errorf("failed to set directory ownership: %w", err)
	}

	return nil
}

func writeCredentialsToFile(credentials []*s3Credentials, pathToCredentialFile string) error {
	file, err := os.Create(pathToCredentialFile)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Write the credentials to disk
	for _, cred := range credentials {
		_, err := file.WriteString(fmt.Sprintf("[%s]\naws_access_key_id=%s\naws_secret_access_key=%s\n\n",
			cred.profile, cred.accessKeyID, cred.secretAccessKey))
		if err != nil {
			return fmt.Errorf("failed to write s3 profile %s: %w", cred.profile, err)
		}
	}

	return file.Sync()
}

func parseCredentialsFromConfigURL(configURL, assignedProfile string) (*s3Credentials, error) {
	barmanURL, err := url.Parse(configURL)
	if err != nil {
		return nil, fmt.Errorf("invalid configURL: %w", err)
	}

	accessKey := barmanURL.User.Username()
	if accessKey == "" {
		return nil, fmt.Errorf("AWS ACCESS KEY is missing")
	}

	secretAccessKey, _ := barmanURL.User.Password()
	if secretAccessKey == "" {
		return nil, fmt.Errorf("AWS SECRET KEY is missing")
	}

	return &s3Credentials{
		profile:         assignedProfile,
		accessKeyID:     accessKey,
		secretAccessKey: secretAccessKey,
	}, nil
}
