package flypg

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/fly-apps/postgres-flex/internal/flypg/admin"
	"github.com/jackc/pgx/v5"
)

func isRegistered(ctx context.Context, conn *pgx.Conn, n *Node) (bool, error) {
	// Short-circuit if we are holding a certificate
	if hasRegistrationCertificate() {
		return true, nil
	}

	// Below is for backwards compatibility
	databases, err := admin.ListDatabases(ctx, conn)
	if err != nil {
		return false, err
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
		return false, err
	}
	defer func() { _ = repConn.Close(ctx) }()

	member, err := n.RepMgr.Member(ctx, conn)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to resolve member role: %s", err)
	}

	if member.Active {
		if err := issueRegistrationCertificate(); err != nil {
			fmt.Println("failed to issue registration certificate.")
			return true, nil
		}
	}

	return true, nil
}

func issueRegistrationCertificate() error {
	return os.WriteFile("/data/.registrationCert", []byte(""), 0600)
}

func hasRegistrationCertificate() bool {
	if _, err := os.Stat("/data/.registrationCert"); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}
