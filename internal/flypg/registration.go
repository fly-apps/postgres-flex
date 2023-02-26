package flypg

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/jackc/pgx/v5"
)

const registrationFile = "/data/.registration"

func isRegistered(ctx context.Context, conn *pgx.Conn, n *Node) (bool, error) {
	// Short-circuit if we are holding a certificate
	if registrationCertExists() {
		return true, nil
	}

	// Below is for backwards compatibility
	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		return false, fmt.Errorf("failed to list databases: %s", err)
	}

	repmgrExists := false
	for _, db := range databases {
		if db.Name == n.RepMgr.DatabaseName {
			repmgrExists = true
			break
		}
	}

	if !repmgrExists {
		return false, nil
	}

	repConn, err := n.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to connect to local repmgr: %s", err)
	}
	defer func() { _ = repConn.Close(ctx) }()

	member, err := n.RepMgr.Member(ctx, repConn)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to resolve member role: %s", err)
	}

	// If we are active, issue registration certificate
	if member.Active {
		if err := issueRegistrationCert(); err != nil {
			log.Println("[WARN] Failed to issue registration certificate.")
			return true, nil
		}
	}

	return true, nil
}

func issueRegistrationCert() error {
	return os.WriteFile(registrationFile, []byte(""), 0600)
}

func registrationCertExists() bool {
	if _, err := os.Stat(registrationFile); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func removeRegistrationCert() error {
	return os.Remove(registrationFile)
}
