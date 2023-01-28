package flypg

import (
	"errors"
	"fmt"
	"testing"
)

func TestZombieDiagnosis(t *testing.T) {

	type TestCase struct {
		MyHostname string
		// Total number of registered members in the cluster. ( including the primary )
		TotalMembers int
		// Total number of members that can be reached over the network. Will never be less than 1.
		TotalActive int
		// Total number of members that were not reachable over the network.
		TotalInactive int
		// This will track each primary conveyed by standbys that is not equal to ourself and with that
		// the total number of occurances.
		ConflictMap map[string]int
		// The expected hostname returned.
		ExpectedHostname string
		// Whether or not we are expected to be a zombie.
		ExpectedZombie bool
	}

	type TestCases struct {
		Cases []TestCase
	}

	tests := TestCases{
		Cases: []TestCase{
			// Test case: 0
			// Single node setup ( Only possible setup )
			{
				MyHostname:       "host-1",
				TotalMembers:     1,
				TotalActive:      1,
				TotalInactive:    0,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "host-1",
				ExpectedZombie:   false,
			},
			// Test case: 1
			// 2 member setup
			{
				MyHostname:       "host-1",
				TotalMembers:     2,
				TotalActive:      2,
				TotalInactive:    0,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "host-1",
				ExpectedZombie:   false,
			},
			// Test case: 2
			// 2 member setup with an inactive standby
			{
				MyHostname:       "host-1",
				TotalMembers:     2,
				TotalActive:      1,
				TotalInactive:    1,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "",
				ExpectedZombie:   true,
			},
			// Test case: 3
			// 2 member setup with the standby reporting different primary
			{
				MyHostname:    "host-1",
				TotalMembers:  2,
				TotalActive:   1,
				TotalInactive: 1,
				ConflictMap: map[string]int{
					"host-1": 1,
				},
				ExpectedHostname: "",
				ExpectedZombie:   true,
			},
			// Test case: 4
			// 3 member setup with 1 standby offline
			{
				MyHostname:       "host-1",
				TotalMembers:     3,
				TotalActive:      2,
				TotalInactive:    1,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "host-1",
				ExpectedZombie:   false,
			},
			// Test case: 5
			// 3 member setup where both standby's are offline.
			{
				MyHostname:       "host-1",
				TotalMembers:     3,
				TotalActive:      1,
				TotalInactive:    2,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "",
				ExpectedZombie:   true,
			},
			// Test case: 6
			// 3 member setup where both standbys agree that i'm not the primary.
			{
				MyHostname:    "host-1",
				TotalMembers:  3,
				TotalActive:   3,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-primary": 2,
				},
				ExpectedHostname: "secret-primary",
				ExpectedZombie:   true,
			},
			// Test case: 7
			// 3 member setup where 1 standby disagrees that i'm primary.
			{
				MyHostname:    "host-1",
				TotalMembers:  3,
				TotalActive:   3,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-primary": 1,
				},
				ExpectedHostname: "host-1",
				ExpectedZombie:   false,
			},
			// Test case: 8
			// 3 member setup where both standbys report different primaries.
			{
				MyHostname:    "host-1",
				TotalMembers:  3,
				TotalActive:   3,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-primary":   1,
					"secret-primary-2": 1,
				},
				ExpectedHostname: "",
				ExpectedZombie:   true,
			},
			// Test case: 9
			// 4 member setup
			{
				MyHostname:       "host-1",
				TotalMembers:     4,
				TotalActive:      4,
				TotalInactive:    0,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "host-1",
				ExpectedZombie:   false,
			},
			// Test case: 10
			// 4 member setup with 1 standby that is inactive
			{
				MyHostname:       "host-1",
				TotalMembers:     4,
				TotalActive:      3,
				TotalInactive:    1,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "host-1",
				ExpectedZombie:   false,
			},
			// Test case: 11
			// 4 member setup with 2 standbys that are inactive ( unable to meet quorum)
			{
				MyHostname:       "host-1",
				TotalMembers:     4,
				TotalActive:      2,
				TotalInactive:    2,
				ConflictMap:      map[string]int{},
				ExpectedHostname: "",
				ExpectedZombie:   true,
			},
			// Test case: 12
			// 4 member setup with 2 standbys agreeing on a different primary.
			{
				MyHostname:    "host-1",
				TotalMembers:  4,
				TotalActive:   4,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-host": 2,
				},
				ExpectedHostname: "",
				ExpectedZombie:   true,
			},
			// Test case: 13
			// 4 member setup with 3 standbys agreeing on a different primary.
			{
				MyHostname:    "host-1",
				TotalMembers:  4,
				TotalActive:   4,
				TotalInactive: 0,
				ConflictMap: map[string]int{
					"secret-host": 3,
				},
				ExpectedHostname: "secret-host",
				ExpectedZombie:   true,
			},
		},
	}

	for i, c := range tests.Cases {

		hostname, err := ZombieDiagnosis(c.MyHostname, c.TotalMembers, c.TotalInactive, c.TotalActive, c.ConflictMap)
		if err != nil {
			if errors.Is(err, ErrZombieDiscovered) {
				if !c.ExpectedZombie {
					fmt.Printf("Hostname: %s, Total members: %d, Total active: %d, Total inactive: %d, Conflicts: %+v\n",
						c.MyHostname,
						c.TotalMembers,
						c.TotalActive,
						c.TotalInactive,
						c.ConflictMap,
					)
					t.Logf("test case: %d failed. Wasn't expecting to be a Zombie", i)
					t.Fail()
					return
				}
				if c.ExpectedHostname != hostname {
					t.Logf("test case %d failed. Expected hostname to be %s, but got %s", i, c.ExpectedHostname, hostname)
					t.Fail()
					return
				}
			}
		} else {
			if c.ExpectedZombie {
				t.Logf("Test case: %d failed. Expected to be a zombie, but wasn't", i)
				t.Fail()
				return
			}
			if c.ExpectedHostname != hostname {
				t.Logf("test case %d failed. Expected hostname to be %s, but got %s", i, c.ExpectedHostname, hostname)
				t.Fail()
				return
			}

		}

	}

}
