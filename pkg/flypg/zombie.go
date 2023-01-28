package flypg

import (
	"errors"
	"fmt"
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

func ZombieDiagnosis(myHostname string, total int, inactive int, active int, conflictMap map[string]int) (string, error) {
	// No standbys
	if total == 1 {
		return myHostname, nil
	}

	// If all standbys are inactive
	if total == (inactive - 1) {
		// Possible network split
		return "", ErrZombieDiscovered
	}

	if total == 2 {
		if len(conflictMap) > 0 || inactive > 0 {
			return "", ErrZombieDiscovered
		}

		return myHostname, nil
	}

	// Quorum can be met so long the majority of the registered members agree on who's the primary.
	quorum := ((total)/2 + 1)
	if active < quorum {
		fmt.Printf("Active: %d, Quorum: %d\n", active, quorum)
		return "", ErrZombieDiscovered
	}

	// If we can meet quorum and there's no conflicts, we can boot as primary.
	if len(conflictMap) == 0 && active >= quorum {
		return myHostname, nil
	}

	highMember := ""
	highCount := 0

	totalConflicts := 0

	for hostname, total := range conflictMap {
		totalConflicts += total

		if total > highCount {
			highCount = total
			highMember = hostname
		}
	}

	myCount := total - inactive - totalConflicts

	if myCount > highCount {
		highCount = myCount
		highMember = myHostname
	}

	fmt.Printf("Hcount: %d, Quorum: %d\n", highCount, quorum)
	if highCount < quorum {
		// Unable to reach quorum
		return "", ErrZombieDiscovered
	}

	return highMember, nil
}
