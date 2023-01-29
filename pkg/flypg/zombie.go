package flypg

import (
	"errors"
	"os"
)

func ZombieLockExists() bool {
	_, err := os.Stat("/data/zombie.lock")
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func writeZombieLock(hostname string) error {
	if err := os.WriteFile("/data/zombie.lock", []byte(hostname), 0644); err != nil {
		return err
	}

	return nil
}

func removeZombieLock() error {
	if err := os.Remove("/data/zombie.lock"); err != nil {
		return err
	}

	return nil
}

func readZombieLock() (string, error) {
	body, err := os.ReadFile("/data/zombie.lock")
	if err != nil {
		return "", err
	}

	return string(body), nil
}

var ErrZombieDiscovered = errors.New("Zombie")

// ZombieDiagnosis takes information about the current cluster state and determines whether it is safe to boot ourself as the primary.
func ZombieDiagnosis(myHostname string, total int, inactive int, active int, conflictMap map[string]int) (string, error) {
	// We can short-circuit a single node cluster.
	if total == 1 {
		return myHostname, nil
	}

	quorum := total/2 + 1

	// Active members must meet quorum.
	if active < quorum {
		return "", ErrZombieDiscovered
	}

	topCandidate := ""
	highestTotal := 0
	totalConflicts := 0

	// Evaluate conflicts and calculate top referenced primary
	for hostname, total := range conflictMap {
		totalConflicts += total

		if total > highestTotal {
			highestTotal = total
			topCandidate = hostname
		}
	}

	// Calculate our references
	myCount := total - inactive - totalConflicts

	if myCount >= quorum {
		return myHostname, nil
	}

	if highestTotal >= quorum {
		return topCandidate, ErrZombieDiscovered
	}

	return "", ErrZombieDiscovered
}
