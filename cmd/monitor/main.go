package main

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/oklog/run"
	"golang.org/x/exp/maps"
)

var (
	deadMembmerMonitorFrequency = time.Minute * 5
	orphanedRSMonitorFrequency  = time.Minute * 30
	splitBrainMonitorFrequency  = time.Minute * 1
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := flypg.NewNode()
	if err != nil {
		panic(err)
	}

	var g run.Group

	g.Add(run.SignalHandler(ctx, os.Interrupt, syscall.SIGTERM))

	// Split brain monitor

	g.Add(func() error {
		ticker := time.NewTicker(splitBrainMonitorFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:

				// On a regular interval we need to do the following against the active primary.

				// Verify quorum

				// If quorum can be met, do nothing.

				// If quorum can not be met, make primary readonly.

			}
		}
	}, func(error) {

	})

	// Orphaned replication slot monitor
	g.Add(func() error {
		ticker := time.NewTicker(orphanedRSMonitorFrequency)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				// TODO - We should monitor a orphaned replication slots over time to ensure
				// we don't race with a newly provisioned standby.
				if err := orphanedReplicationSlotTick(ctx, node); err != nil {
					fmt.Println(err)
				}
			}
		}
	}, func(error) {

	})

	// Dead member monitor
	g.Add(func() error {
		internal, err := flypg.ReadFromFile("/data/flypg.internal.conf")
		if err != nil {
			fmt.Printf("failed to open config: %s\n", err)
			os.Exit(1)
		}

		user, err := flypg.ReadFromFile("/data/flypg.user.conf")
		if err != nil {
			fmt.Printf("failed to open config: %s\n", err)
			os.Exit(1)
		}

		maps.Copy(user, internal)

		deadMemberRemovalThreshold, err := time.ParseDuration(fmt.Sprint(internal["standby_clean_interval"]))
		if err != nil {
			fmt.Printf(fmt.Sprintf("Failed to parse config: %s", err))
			os.Exit(1)
		}

		fmt.Printf("Pruning every %s...\n", deadMemberRemovalThreshold)

		seenAt := map[int]time.Time{}

		ticker := time.NewTicker(deadMembmerMonitorFrequency)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if err := deadMemberMonitorTick(ctx, node, seenAt, deadMemberRemovalThreshold); err != nil {
					fmt.Println(err)
				}
			}
		}

	}, func(error) {

	})

}

func splitBrainMonitor(ctx context.Context, node *flypg.Node) error {
	// On a regular interval we need to do the following against the active primary.
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection with local db: %s", err)
	}

	member, err := node.RepMgr.Member(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to resolve repmgr member: %s", err)
	}
	// Verify quorum

	// If quorum can be met, do nothing.

	// If quorum can not be met, make primary readonly.

	return nil
}
