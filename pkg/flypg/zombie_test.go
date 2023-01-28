package flypg

import (
	"errors"
	"fmt"
	"testing"
)

func TestZombieEval(t *testing.T) {

	type TestCase struct {
		MyHostname   string
		totalMembers int
		// Total active members. This will never be less than 1.
		TotalActive int
		// Total inactive members, will never include self.
		TotalInactive int
		// Conflict map includes a list of primaries that differ from self
		// and the number of occurrences found.
		ConflictMap map[string]int

		ExpectedHostname string
	}

	type TestCases struct {
		Cases []TestCase
	}

	tests := TestCases{
		Cases: []TestCase{
			// Single node setup ( Only possible setup )
			{
				MyHostname:       "current-primary",
				totalMembers:     1,
				TotalActive:      1,
				TotalInactive:    0,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "current-primary",
			},
			// 2 member setup
			{
				MyHostname:       "current-primary",
				totalMembers:     2,
				TotalActive:      2,
				TotalInactive:    0,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "current-primary",
			},
			// 2 member setup with an inactive standby
			{
				MyHostname:       "current-primary",
				totalMembers:     2,
				TotalActive:      1,
				TotalInactive:    1,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "zombie",
			},
			// 2 member setup with the standby reporting different primary
			{
				MyHostname:    "current-primary",
				totalMembers:  2,
				TotalActive:   1,
				TotalInactive: 1,
				ConflictMap: map[string]int{
					"host-1": 1,
				},
				ExpectedHostname: "zombie",
			},
			// 3 member setup with 1 standby offline
			{
				MyHostname:       "current-primary",
				totalMembers:     3,
				TotalActive:      2,
				TotalInactive:    1,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "current-primary",
			},
			// 3 member setup where both standby's are offline.
			{
				MyHostname:       "current-primary",
				totalMembers:     3,
				TotalActive:      1,
				TotalInactive:    2,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "zombie",
			},
			// 3 member setup where both standbys agree that i'm not the primary.
			{
				MyHostname:    "current-primary",
				totalMembers:  3,
				TotalActive:   3,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-primary": 2,
				},
				ExpectedHostname: "secret-primary",
			},
			// 3 member setup where 1 standby disagrees that i'm primary.
			{
				MyHostname:    "current-primary",
				totalMembers:  3,
				TotalActive:   3,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-primary": 1,
				},
				ExpectedHostname: "current-primary",
			},
			// 3 member setup where both standbys report different primaries.
			{
				MyHostname:    "current-primary",
				totalMembers:  3,
				TotalActive:   3,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-primary":   1,
					"secret-primary-2": 1,
				},
				ExpectedHostname: "zombie",
			},
		},
	}

	for i, c := range tests.Cases {

		hostname, err := ZombieEval(c.MyHostname, c.totalMembers, c.TotalInactive, c.TotalActive, c.ConflictMap)
		fmt.Println(hostname)
		if err != nil {
			if errors.Is(err, ErrZombieDiscovered) {
				if c.ExpectedHostname != "zombie" {
					t.Logf("test case %d failed: %+v . Result: zombie", i, c)
					t.Fail()
					return
				}
			}
		} else {
			if c.ExpectedHostname != hostname {
				t.Logf("test case %d expected zombie, wasn't: %+v . Result: %s", i, c, hostname)
				t.Fail()
			}

		}

	}

}
