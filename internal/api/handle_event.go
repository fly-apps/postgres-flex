package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/jackc/pgx/v5"
)

type EventRequest struct {
	Name    string `json:"name"`
	NodeID  int    `json:"nodeID"`
	Success bool   `json:"success"`
	Details string `json:"details"`
}

const (
	childNodeDisconnect = "child_node_disconnect"
	childNodeReconnect  = "child_node_reconnect"
	childNodeNewConnect = "child_node_new_connect"
)

func handleEvent(w http.ResponseWriter, r *http.Request) {
	var event EventRequest
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		log.Printf("[ERROR] Failed to decode event request: %s\n", err)
		renderErr(w, err)
		return
	}
	defer func() { _ = r.Body.Close() }()

	if !event.Success {
		errMsg := fmt.Sprintf("[ERROR] Event %q failed: %s", event.Name, event.Details)
		log.Println(errMsg)
		renderErr(w, errors.New(errMsg))
		return
	}

	if err := processEvent(r.Context(), event); err != nil {
		log.Printf("[ERROR] Failed to process event: %s\n", err)
		renderErr(w, err)
		return
	}
}

func processEvent(ctx context.Context, event EventRequest) error {
	log.Printf("Processing event: %q \n", event.Name)
	node, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %s", err)
	}

	switch event.Name {
	case childNodeDisconnect, childNodeReconnect, childNodeNewConnect:
		conn, err := node.RepMgr.NewLocalConnection(ctx)
		if err != nil {
			return fmt.Errorf("failed to open local connection: %s", err)
		}
		defer func() { _ = conn.Close(ctx) }()

		member, err := node.RepMgr.Member(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to resolve member: %s", err)
		}

		if member.Role != flypg.PrimaryRoleName {
			return nil
		}

		if err := evaluateClusterState(ctx, conn, node); err != nil {
			return fmt.Errorf("failed to evaluate cluster state: %s", err)
		}
	}

	return nil
}

// TODO Move this into zombie.go
func evaluateClusterState(ctx context.Context, conn *pgx.Conn, node *flypg.Node) error {
	primary, err := flypg.PerformScreening(ctx, conn, node)
	if errors.Is(err, flypg.ErrZombieDiagnosisUndecided) || errors.Is(err, flypg.ErrZombieDiscovered) {
		if err := flypg.Quarantine(ctx, node, primary); err != nil {
			return fmt.Errorf("failed to quarantine failed primary: %s", err)
		}
		log.Println("[WARN] Primary is going read-only to protect against potential split-brain")
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to run zombie diagnosis: %s", err)
	}

	// Clear zombie lock if it exists
	if flypg.ZombieLockExists() {
		log.Println("Quorom has been reached. Disabling read-only mode.")
		if err := flypg.RemoveZombieLock(); err != nil {
			return fmt.Errorf("failed to remove zombie lock file: %s", err)
		}

		if err := flypg.BroadcastReadonlyChange(ctx, node, false); err != nil {
			log.Printf("failed to disable readonly: %s", err)
		}
	}

	return nil
}
