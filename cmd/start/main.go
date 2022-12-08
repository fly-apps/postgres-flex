package main

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-standalone/pkg/flypg"
	"github.com/fly-apps/postgres-standalone/pkg/supervisor"
)

func main() {
	requiredPasswords := []string{"SU_PASSWORD", "OPERATOR_PASSWORD"}
	for _, str := range requiredPasswords {
		if _, exists := os.LookupEnv(str); !exists {
			panic(fmt.Errorf("%s is required", str))
		}
	}

	node, err := flypg.NewNode()
	if err != nil {
		panicHandler(err)
	}

	if err = node.Init(); err != nil {
		panicHandler(err)
	}

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for range t.C {

			if err := node.PostInit(); err != nil {
				fmt.Printf("failed post-init: %s", err)
				continue
			}

			return
		}
	}()

	env := map[string]string{
		"PGDATA":     os.Getenv("PGDATA"),
		"PGPASSFILE": os.Getenv("PGPASSFILE"),
		"PATH":       os.Getenv("PATH"),
	}

	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("flypg", "gosu postgres postgres -D /data/postgresql/")
	svisor.AddProcess("repmgrd", "gosu postgres repmgrd -f /data/repmgr.conf --daemonize=false -v", supervisor.WithEnv(env), supervisor.WithRestart(0, 5*time.Second))
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
