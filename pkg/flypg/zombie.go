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

	quorum := total/2 + 1

	// Active members must at least equal quorum in order for primary resolution to be succcessful.
	if active < quorum {
		return "", ErrZombieDiscovered
	}

	topCandidate := ""
	highestTotal := 0
	totalConflicts := 0

	// Evaluate conflicts to calculate the highest referrenced primary
	for hostname, total := range conflictMap {
		totalConflicts += total

		if total > highestTotal {
			highestTotal = total
			topCandidate = hostname
		}
	}

	myCount := total - inactive - totalConflicts

	if myCount >= quorum {
		return myHostname, nil
	}

	if highestTotal >= quorum {
		return topCandidate, ErrZombieDiscovered
	}

	return "", ErrZombieDiscovered
}
