package main

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-flex/pkg/flypg"
	"github.com/fly-apps/postgres-flex/pkg/supervisor"
)

func main() {
	requiredPasswords := []string{"SU_PASSWORD", "OPERATOR_PASSWORD", "REPL_PASSWORD"}
	for _, str := range requiredPasswords {
		if _, exists := os.LookupEnv(str); !exists {
			panic(fmt.Errorf("%s is required", str))
		}
	}

	node, err := flypg.NewNode()
	if err != nil {
		panicHandler(err)
	}

	ctx := context.Background()

	if err = node.Init(ctx); err != nil {
		panicHandler(err)
	}

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for range t.C {

			if err := node.PostInit(ctx); err != nil {
				fmt.Printf("failed post-init: %s. Retrying...\n", err)
				continue
			}
			return
		}
	}()

	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("flypg", fmt.Sprintf("gosu postgres postgres -D %s -p %d", node.DataDir, node.Port))

	svisor.AddProcess("pgbouncer", fmt.Sprintf("pgbouncer %s/pgbouncer.ini", node.PGBouncer.ConfigPath),
		supervisor.WithRestart(0, 1*time.Second),
	)
	svisor.AddProcess("repmgrd", fmt.Sprintf("gosu postgres repmgrd -f %s --daemonize=false", node.RepMgr.ConfigPath),
		supervisor.WithRestart(0, 5*time.Second),
	)

	exporterEnv := map[string]string{
		"DATA_SOURCE_URI":                     fmt.Sprintf("[%s]:%d/postgres?sslmode=disable", node.PrivateIP, node.Port),
		"DATA_SOURCE_USER":                    node.SUCredentials.Username,
		"DATA_SOURCE_PASS":                    node.SUCredentials.Password,
		"PG_EXPORTER_EXCLUDE_DATABASE":        "template0,template1",
		"PG_EXPORTER_AUTO_DISCOVER_DATABASES": "true",
	}

	svisor.AddProcess("exporter", "postgres_exporter", supervisor.WithEnv(exporterEnv), supervisor.WithRestart(0, 1*time.Second))

	svisor.StopOnSignal(syscall.SIGINT, syscall.SIGTERM)
	svisor.StartHttpListener()

	if err := svisor.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}

func panicHandler(err error) {
	debug := os.Getenv("DEBUG")
	if debug != "" {
		fmt.Println(err.Error())
		fmt.Println("Entering debug mode... (Timeout: 10 minutes")
		time.Sleep(time.Minute * 10)
	}

	panic(err)
}
