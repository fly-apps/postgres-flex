package flypg

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	return !os.IsNotExist(err)
}

func writeZombieLock(hostname string) error {
	if err := os.WriteFile(zombieLockFile, []byte(hostname), 0600); err != nil {
		return err
	}

	if err := utils.SetFileOwnership(zombieLockFile, "postgres"); err != nil {
		return fmt.Errorf("failed to set file ownership: %s", err)
	}

	return nil
}

func RemoveZombieLock() error {
	return os.Remove(zombieLockFile)
}

func ReadZombieLock() (string, error) {
	body, err := os.ReadFile(zombieLockFile)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func PerformScreening(ctx context.Context, conn *pgx.Conn, n *Node) (string, error) {
	members, err := n.RepMgr.VotingMembers(ctx, conn)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("failed to query standbys")
		}
	}

	sample, err := TakeDNASample(ctx, n, members)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate cluster data: %s", err)
	}

	log.Println(DNASampleString(sample))

	return ZombieDiagnosis(sample)
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
		hostname:       node.Hostname(),
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
			log.Printf("[WARN] Failed to connect to %s\n", standby.Hostname)
			sample.totalInactive++
			continue
		}
		defer func() { _ = mConn.Close(ctx) }()

		// Verify the primary
		primary, err := node.RepMgr.PrimaryMember(ctx, mConn)
		if err != nil {
			log.Printf("[WARN] Failed to resolve primary from standby %s\n", standby.Hostname)
			sample.totalInactive++
			continue
		}

		if err := mConn.Close(ctx); err != nil {
			return nil, fmt.Errorf("failed to close connection: %s", err)
		}

		sample.totalActive++

		// Record conflict when primary name does not match our machine ID
		if primary.Hostname != node.Hostname() && primary.Hostname != node.PrivateIP {
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

func Quarantine(ctx context.Context, n *Node, primary string) error {
	if err := writeZombieLock(primary); err != nil {
		return fmt.Errorf("failed to set zombie lock: %s", err)
	}

	if err := BroadcastReadonlyChange(ctx, n, true); err != nil {
		return fmt.Errorf("failed to set read-only: %s", err)
	}

	return nil
}

func DNASampleString(s *DNASample) string {
	return fmt.Sprintf("Voting member(s): %d, Active: %d, Inactive: %d, Conflicts: %d",
		s.totalMembers,
		s.totalActive,
		s.totalInactive,
		s.totalConflicts,
	)
}

func handleZombieLock(ctx context.Context, n *Node) error {
	log.Println("[WARN] Zombie lock detected!")
	primaryStr, err := ReadZombieLock()
	if err != nil {
		return fmt.Errorf("failed to read zombie lock: %s", primaryStr)
	}

	// If the zombie lock contains a hostname, it means we were able to
	// resolve the real primary and will attempt to rejoin it.
	if primaryStr != "" {
		conn, err := n.RepMgr.NewRemoteConnection(ctx, primaryStr)
		if err != nil {
			return fmt.Errorf("failed to establish a connection to our rejoin target %s: %s", primaryStr, err)
		}
		defer func() { _ = conn.Close(ctx) }()

		primary, err := n.RepMgr.PrimaryMember(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to confirm primary on recover target %s: %s", primaryStr, err)
		}

		// Confirm that our rejoin target still identifies itself as the primary.
		if primary.Hostname != primaryStr {
			// Clear the zombie.lock file so we can attempt to re-resolve the correct primary.
			if err := RemoveZombieLock(); err != nil {
				return fmt.Errorf("failed to remove zombie lock: %s", err)
			}

			return ErrZombieLockPrimaryMismatch
		}

		// If the primary does not reside within our primary region, we cannot rejoin until it is.
		if primary.Region != n.PrimaryRegion {
			log.Printf("[WARN] Primary region mismatch detected. The primary lives in '%s', while PRIMARY_REGION is set to '%s'\n", primary.Region, n.PrimaryRegion)
			return ErrZombieLockRegionMismatch
		}

		if err := n.RepMgr.rejoinCluster(primary.Hostname); err != nil {
			return fmt.Errorf("failed to rejoin cluster: %s", err)
		}

		// TODO - Wait for target cluster to register self as a standby.

		if err := RemoveZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock: %s", err)
		}

		// Ensure the single instance created with the --force-rewind process is cleaned up properly.
		_, err = utils.RunCommand("pg_ctl -D /data/postgresql/ stop", "postgres")
		if err != nil {
			return fmt.Errorf("failed to stop postgres: %s", err)
		}
	} else {
		// TODO - Provide link to documentation on how to address this
		log.Println("[WARN] Zombie lock file does not contain a hostname.")
		log.Println("[WARN] This likely means that we were unable to determine who the real primary is.")
		log.Println("[WARN] If a new primary has been established, consider adding a new replica with `fly machines clone <primary-machine-id>` and then remove this member.")
	}

	return nil
}

func EvaluateClusterState(ctx context.Context, conn *pgx.Conn, node *Node) error {
	primary, err := PerformScreening(ctx, conn, node)
	if errors.Is(err, ErrZombieDiagnosisUndecided) || errors.Is(err, ErrZombieDiscovered) {
		if err := Quarantine(ctx, node, primary); err != nil {
			return fmt.Errorf("failed to quarantine failed primary: %s", err)
		}
		log.Println("[WARN] Primary is going read-only to protect against potential split-brain")
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to run zombie diagnosis: %s", err)
	}

	// Clear zombie lock if it exists
	if ZombieLockExists() {
		log.Println("Quorum has been reached. Disabling read-only mode.")
		if err := RemoveZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock file: %s", err)
		}

		if err := BroadcastReadonlyChange(ctx, node, false); err != nil {
			log.Printf("failed to disable readonly: %s", err)
		}
	}

	return nil
}
