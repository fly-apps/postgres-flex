package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/supervisor"
)

func main() {
	log.SetFlags(0)

	requiredPasswords := []string{"SU_PASSWORD", "OPERATOR_PASSWORD", "REPL_PASSWORD"}
	for _, str := range requiredPasswords {
		if _, exists := os.LookupEnv(str); !exists {
			panic(fmt.Errorf("%s is required", str))
		}
	}

	node, err := flypg.NewNode()
	if err != nil {
		panicHandler(err)
		return
	}

	ctx := context.Background()

	if err = node.Init(ctx); err != nil {
		panicHandler(err)
		return
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

	if timeout, exists := os.LookupEnv("FLY_SCALE_TO_ZERO"); exists {
		duration, err := time.ParseDuration(timeout)
		if err != nil {
			fmt.Printf("failed to parse FLY_SCALE_TO_ZERO duration %s", err)
		} else {
			go func() {
				timeout := time.After(duration)
				for {
					select {
					case <-timeout:
						svisor.Stop()
						os.Exit(0)
					}
				}
			}()
		}
	}

	svisor.AddProcess("postgres", fmt.Sprintf("gosu postgres postgres -D %s -p %d", node.DataDir, node.Port))

	proxyEnv := map[string]string{
		"FLY_APP_NAME":      os.Getenv("FLY_APP_NAME"),
		"PRIMARY_REGION":    os.Getenv("PRIMARY_REGION"),
		"PG_LISTEN_ADDRESS": node.PrivateIP,
	}
	svisor.AddProcess("proxy", "/usr/sbin/haproxy -W -db -f /fly/haproxy.cfg", supervisor.WithEnv(proxyEnv), supervisor.WithRestart(0, 1*time.Second))

	svisor.AddProcess("repmgrd", fmt.Sprintf("gosu postgres repmgrd -f %s --daemonize=false", node.RepMgr.ConfigPath),
		supervisor.WithRestart(0, 5*time.Second),
	)
	svisor.AddProcess("monitor", "/usr/local/bin/start_monitor",
		supervisor.WithRestart(0, 5*time.Second),
	)
	svisor.AddProcess("admin", "/usr/local/bin/start_admin_server",
		supervisor.WithRestart(0, 5*time.Second),
	)

	exporterEnv := map[string]string{
		"DATA_SOURCE_URI":                     fmt.Sprintf("[%s]:%d/postgres?sslmode=disable", node.PrivateIP, node.Port),
		"DATA_SOURCE_USER":                    node.SUCredentials.Username,
		"DATA_SOURCE_PASS":                    node.SUCredentials.Password,
		"PG_EXPORTER_EXCLUDE_DATABASE":        "template0,template1",
		"PG_EXPORTER_AUTO_DISCOVER_DATABASES": "true",
		"PG_EXPORTER_EXTEND_QUERY_PATH":       "/fly/queries.yaml",
	}
	svisor.AddProcess("exporter", "postgres_exporter --log.level=warn ",
		supervisor.WithEnv(exporterEnv),
		supervisor.WithRestart(0, 1*time.Second),
	)

	svisor.StopOnSignal(syscall.SIGINT, syscall.SIGTERM)

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
