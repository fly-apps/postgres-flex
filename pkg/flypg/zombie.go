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

// ZombieDiagnosis takes information about the current cluster state and does two things:
//  1. Determines whether or not it's safe to boot ourself as the primary.
//  2. Try and build a consensus around who the real primary is.
func ZombieDiagnosis(myHostname string, total int, inactive int, active int, conflictMap map[string]int) (string, error) {
	// Single node cluster
	if total == 1 {
		return myHostname, nil
	}

	// Two node cluster
	if total == 2 {
		if len(conflictMap) > 0 || inactive == 1 {
			return "", ErrZombieDiscovered
		}

		return myHostname, nil
	}

	// If all standbys are inactive, then there's a possible network split.
	if total == (inactive - 1) {
		return "", ErrZombieDiscovered
	}

	quorum := ((total)/2 + 1)

	// if we have enough active nodes to meet quorum and there are no conflicts
	// we can assume we are the primary.
	if len(conflictMap) == 0 && active >= quorum {
		return myHostname, nil
	}

	// If there are not enough active nodes to meet quorum we have to assume we are a zombie.
	if active < quorum {
		return "", ErrZombieDiscovered
	}

	topCandidate := ""
	highestCount := 0
	totalConflicts := 0

	// Calculate total conflicts + hightest reported primary
	for hostname, total := range conflictMap {
		totalConflicts += total

		if total > highestCount {
			highestCount = total
			topCandidate = hostname
		}
	}

	// Determine our count
	myCount := total - inactive - totalConflicts

	if myCount > highestCount && myCount >= quorum {
		return myHostname, nil
	}

	// verify our highest count against quorum.
	if highestCount < quorum {
		return "", ErrZombieDiscovered
	}

	return topCandidate, ErrZombieDiscovered
}
