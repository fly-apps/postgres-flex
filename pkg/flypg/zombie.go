package flypg

import (
	"errors"
	"io/ioutil"
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
	if err := ioutil.WriteFile("/data/zombie.lock", []byte(hostname), 0644); err != nil {
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
	body, err := ioutil.ReadFile("/data/zombie.lock")
	if err != nil {
		return "", err
	}

	return string(body), nil
}

var ErrZombieDiscovered = errors.New("Zombie")

// ZombieDiagnosis takes information about the current cluster state and determines whether it is safe to boot ourself as the primary.
func ZombieDiagnosis(myHostname string, total int, inactive int, active int, conflictMap map[string]int) (string, error) {
	// Single node cluster
	if total == 1 {
		return myHostname, nil
	}

	// Two node cluster
	if total == 2 {
		// We can't know if we are the true primary if our only standby is inactive or has conflicts.
		if len(conflictMap) > 0 || inactive == 1 {
			return "", ErrZombieDiscovered
		}

		return myHostname, nil
	}

	// If all standbys are inactive, then we hae to assume a possible network split.
	if total == (inactive - 1) {
		return "", ErrZombieDiscovered
	}

	quorum := (total/2 + 1)

	// if we have enough active nodes to meet quorum and there are no conflicts we are the primary.
	if len(conflictMap) == 0 && active >= quorum {
		return myHostname, nil
	}

	if active < quorum {
		return "", ErrZombieDiscovered
	}

	topCandidate := ""
	highestCount := 0
	totalConflicts := 0

	// Evaluate conflicts to calculate the highest referrenced primary
	for hostname, total := range conflictMap {
		totalConflicts += total

		if total > highestCount {
			highestCount = total
			topCandidate = hostname
		}
	}

	// Calculate our count
	myCount := total - inactive - totalConflicts

	// If we meet quorum, we are done here.
	if myCount >= quorum {
		return myHostname, nil
	}

	// If our highest
	if highestCount < quorum {
		return "", ErrZombieDiscovered
	}

	return topCandidate, ErrZombieDiscovered
}
