package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flybarman"
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

	// Deprecated - We are moving away from having a separate barman Machine
	if os.Getenv("IS_BARMAN") != "" {
		node, err := flybarman.NewNode()
		if err != nil {
			panicHandler(err)
			return
		}

		ctx := context.Background()

		if err = node.Init(ctx); err != nil {
			panicHandler(err)
			return
		}

		svisor := supervisor.New("flybarman", 1*time.Minute)
		svisor.AddProcess("cron", "/usr/sbin/cron -f", supervisor.WithRestart(0, 5*time.Second))
		svisor.AddProcess("barman", fmt.Sprintf("tail -f %s", node.LogFile))
		svisor.AddProcess("admin", "/usr/local/bin/start_admin_server",
			supervisor.WithRestart(0, 5*time.Second),
		)

		svisor.StopOnSignal(syscall.SIGINT, syscall.SIGTERM)

		if err := svisor.Run(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		return
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

	go func() {
		if err := scaleToZeroWorker(ctx, node); err != nil {
			svisor.Stop()
			os.Exit(0)
		}
	}()

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

func scaleToZeroWorker(ctx context.Context, node *flypg.Node) error {
	rawTimeout, exists := os.LookupEnv("FLY_SCALE_TO_ZERO")
	if !exists {
		return nil
	}

	duration, err := time.ParseDuration(rawTimeout)
	if err != nil {
		fmt.Printf("failed to parse FLY_SCALE_TO_ZERO duration %s\n", err)
		return nil
	}

	fmt.Printf("Configured scale to zero with duration of %s\n", duration.String())

	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			current, err := getCurrentConnCount(ctx, node)
			if err != nil {
				log.Printf("Failed to get current connection count will try again in %s\n", duration.String())
				continue
			}
			log.Printf("Current connection count is %d\n", current)
			if current > 1 {
				continue
			}
			return fmt.Errorf("scale to zero condition hit")
		}
	}
}

func getCurrentConnCount(ctx context.Context, node *flypg.Node) (int, error) {
	const sql = "select count(*) from pg_stat_activity where usename != 'repmgr' and usename != 'flypgadmin' and backend_type = 'client backend';"
	conn, err := node.NewLocalConnection(ctx, "postgres", node.OperatorCredentials)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()

	var current int
	if err := conn.QueryRow(ctx, sql).Scan(&current); err != nil {
		return 0, err
	}
	return current, nil
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
