package flypg

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrZombieLockRegionMismatch - The region associated with the resolved primary does not match our PRIMARY_REGION.
	ErrZombieLockRegionMismatch = errors.New("resolved primary does not reside within our PRIMARY_REGION")
	// ErrZombieLockPrimaryMismatch - The primary listed within the zombie.lock file is no longer identifying
	// itself as the primary.
	ErrZombieLockPrimaryMismatch = errors.New("the primary listed in the zombie.lock file is no longer valid")
	// ErrZombieDiscovered - The majority of registered members indicated a different primary.
	ErrZombieDiscovered = errors.New("majority of registered members confirmed we are not the real primary")
	// ErrZombieDiagnosisUndecided - We were unable to determine who the true primary is.
	ErrZombieDiagnosisUndecided = errors.New("unable to confirm we are the true primary")
)

const zombieLockFile = "/data/zombie.lock"

func ZombieLockExists() bool {
	_, err := os.Stat(zombieLockFile)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func writeZombieLock(hostname string) error {
	if err := os.WriteFile(zombieLockFile, []byte(hostname), 0644); err != nil {
		return err
	}

	pgUID, pgGID, err := utils.SystemUserIDs("postgres")
	if err != nil {
		return err
	}

	if err := os.Chown(zombieLockFile, pgUID, pgGID); err != nil {
		return fmt.Errorf("failed to set zombie.lock owner: %s", err)
	}

	return nil
}

func RemoveZombieLock() error {
	if err := os.Remove(zombieLockFile); err != nil {
		return err
	}

	return nil
}

func ReadZombieLock() (string, error) {
	body, err := os.ReadFile(zombieLockFile)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

type DNASample struct {
	hostname       string
	totalMembers   int
	totalActive    int
	totalInactive  int
	totalConflicts int
	conflictMap    map[string]int
}

func TakeDNASample(ctx context.Context, node *Node, standbys []Member) (*DNASample, error) {
	sample := &DNASample{
		hostname:       node.PrivateIP,
		totalMembers:   len(standbys) + 1,
		totalActive:    1,
		totalInactive:  0,
		totalConflicts: 0,
		conflictMap:    map[string]int{},
	}

	for _, standby := range standbys {
		// Check for connectivity
		mConn, err := node.RepMgr.NewRemoteConnection(ctx, standby.Hostname)
		if err != nil {
			fmt.Printf("failed to connect to %s\n", standby.Hostname)
			sample.totalInactive++
			continue
		}
		defer mConn.Close(ctx)

		// Verify the primary
		primary, err := node.RepMgr.PrimaryMember(ctx, mConn)
		if err != nil {
			fmt.Printf("failed to resolve primary from standby %s\n", standby.Hostname)
			sample.totalInactive++
			continue
		}

		sample.totalActive++

		// Record conflict when primary doesn't match.
		if primary.Hostname != node.PrivateIP {
			sample.totalConflicts++
			sample.conflictMap[primary.Hostname]++
		}
	}

	return sample, nil
}

func ZombieDiagnosis(s *DNASample) (string, error) {
	// We can short-circuit a single node cluster.
	if s.totalMembers == 1 {
		return s.hostname, nil
	}

	quorum := s.totalMembers/2 + 1

	if s.totalActive < quorum {
		return "", ErrZombieDiagnosisUndecided
	}

	topCandidate := ""
	highestTotal := 0

	// Evaluate conflicts and calculate top referenced primary
	for hostname, total := range s.conflictMap {
		if total > highestTotal {
			highestTotal = total
			topCandidate = hostname
		}
	}

	// Calculate our references
	myCount := s.totalMembers - s.totalInactive - s.totalConflicts

	// We have to fence the primary in case the active cluster is in the middle of a failover.
	if myCount >= quorum {
		if s.totalConflicts > 0 {
			return "", ErrZombieDiagnosisUndecided
		}
		return s.hostname, nil
	}

	if highestTotal >= quorum {
		return topCandidate, ErrZombieDiscovered
	}

	return "", ErrZombieDiagnosisUndecided
}

func Quarantine(ctx context.Context, conn *pgx.Conn, n *Node, primary string) error {
	if err := writeZombieLock(primary); err != nil {
		return fmt.Errorf("failed to set zombie lock: %s", err)
	}

	if err := BroadcastReadonlyChange(ctx, n, true); err != nil {
		return fmt.Errorf("failed to set read-only: %s", err)
	}

	return nil
}

func DNASampleString(s *DNASample) string {
	return fmt.Sprintf("Registered members: %d, Active member(s): %d, Inactive member(s): %d, Conflicts detected: %d",
		s.totalMembers,
		s.totalActive,
		s.totalInactive,
		s.totalConflicts,
	)
}
