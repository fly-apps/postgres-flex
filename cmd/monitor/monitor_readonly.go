package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
)

type readonlyStateResponse struct {
	Result bool
}

func monitorReadOnly(ctx context.Context, node *flypg.Node) {
	ticker := time.NewTicker(readonlyStateMonitorFrequency)
	defer ticker.Stop()
	for range ticker.C {
		if err := readonlyMonitorTick(ctx, node); err != nil {
			log.Printf("readOnlyMonitorTick failed with: %s", err)
		}
	}
}

func readonlyMonitorTick(ctx context.Context, node *flypg.Node) error {
	conn, err := node.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to open local connection: %s", err)
	}
	defer conn.Close(ctx)

	member, err := node.RepMgr.Member(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query local member: %s", err)
	}

	if member.Role == flypg.PrimaryRoleName {
		return nil
	}

	primary, err := node.RepMgr.PrimaryMember(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to query primary member: %s", err)
	}

	endpoint := fmt.Sprintf("http://[%s]:5500/%s", primary.Hostname, flypg.ReadOnlyStateEndpoint)
	resp, err := http.Get(endpoint)
	if err != nil {
		return fmt.Errorf("failed to query primary readonly state: %s", err)
	}
	defer resp.Body.Close()

	var state readonlyStateResponse
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode result: %s", err)
	}

	if state.Result {
		if !flypg.ReadOnlyLockExists() {
			log.Printf("Setting connections running under %s to readonly\n", node.PrivateIP)
			if err := flypg.EnableReadonly(ctx, node); err != nil {
				return fmt.Errorf("failed to set connection under %s to readonly: %s", node.PrivateIP, err)
			}
		}
	} else {
		if !flypg.ZombieLockExists() && flypg.ReadOnlyLockExists() {
			log.Printf("Setting connections running under %s to read/write\n", node.PrivateIP)
			if err := flypg.DisableReadonly(ctx, node); err != nil {
				return fmt.Errorf("failed to set connections under %s read/write: %s", node.PrivateIP, err)
			}
		}
	}

	return nil
}
