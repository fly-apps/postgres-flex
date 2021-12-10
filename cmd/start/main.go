package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-standalone/pkg/flypg"
	"github.com/fly-apps/postgres-standalone/pkg/flypg/admin"
	"github.com/fly-apps/postgres-standalone/pkg/supervisor"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/mem"
)

func main() {
	requiredPasswords := []string{"SU_PASSWORD", "OPERATOR_PASSWORD"}
	for _, str := range requiredPasswords {
		if _, exists := os.LookupEnv(str); !exists {
			panic(fmt.Errorf("%s is required", str))
		}
	}

	// Ensure PG files have correct ownership.
	setDirOwnership()

	if _, err := os.Stat("/data/postgres"); os.IsNotExist(err) {
		if err := initializePostgres(); err != nil {
			PanicHandler(err)
		}
	}

	if err := setDefaultHBA(); err != nil {
		PanicHandler(err)
	}

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		fmt.Println("Configuring users")

		for range t.C {
			if err := createRequiredUsers(); err != nil {
				fmt.Printf("Failed to create required users: %s\n", err.Error())
				continue
			}

			fmt.Println("Successfully created users")

			return
		}
	}()

	node, err := flypg.NewNode()
	if err != nil {
		PanicHandler(err)
	}

	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("flypg", "gosu postgres postgres -D /data/postgres/")

	exporterEnv := map[string]string{
		"DATA_SOURCE_URI":                      fmt.Sprintf("[%s]:%d/postgres?sslmode=disable", node.PrivateIP, node.PGPort),
		"DATA_SOURCE_USER":                     node.SUCredentials.Username,
		"DATA_SOURCE_PASS":                     node.SUCredentials.Password,
		"PG_EXPORTER_EXCLUDE_DATABASE":         "template0,template1",
		"PG_EXPORTER_DISABLE_SETTINGS_METRICS": "true",
		"PG_EXPORTER_AUTO_DISCOVER_DATABASES":  "true",
		"PG_EXPORTER_EXTEND_QUERY_PATH":        "/fly/queries.yaml",
	}

	svisor.AddProcess("exporter", "postgres_exporter", supervisor.WithEnv(exporterEnv), supervisor.WithRestart(0, 1*time.Second))

	svisor.StopOnSignal(syscall.SIGINT, syscall.SIGTERM)
	svisor.StartHttpListener()

	if err := svisor.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}

func initializePostgres() error {
	fmt.Println("Initializing Postgres")

	if err := ioutil.WriteFile("/data/.default_password", []byte(os.Getenv("OPERATOR_PASSWORD")), 0644); err != nil {
		return err
	}

	cmd := exec.Command("gosu", "postgres", "initdb", "--pgdata", "/data/postgres", "--pwfile=/data/.default_password")
	_, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}

	return nil
}

func setDirOwnership() {
	pgUser, err := user.Lookup("postgres")
	if err != nil {
		PanicHandler(err)
	}
	pgUID, err := strconv.Atoi(pgUser.Uid)
	if err != nil {
		PanicHandler(err)
	}
	pgGID, err := strconv.Atoi(pgUser.Gid)
	if err != nil {
		PanicHandler(err)
	}

	cmdStr := fmt.Sprintf("chown -R %d:%d %s", pgUID, pgGID, "/data")
	cmd := exec.Command("sh", "-c", cmdStr)
	_, err = cmd.Output()
	if err != nil {
		PanicHandler(err)
	}
}

type HBAEntry struct {
	Type     string
	Database string
	User     string
	Address  string
	Method   string
}

func setDefaultHBA() error {
	var entries []HBAEntry

	entries = append(entries, HBAEntry{
		Type:     "local",
		Database: "all",
		User:     "postgres",
		Method:   "trust",
	})

	entries = append(entries, HBAEntry{
		Type:     "host",
		Database: "all",
		User:     "all",
		Address:  "0.0.0.0/0",
		Method:   "md5",
	})

	entries = append(entries, HBAEntry{
		Type:     "host",
		Database: "all",
		User:     "all",
		Address:  "::0/0",
		Method:   "md5",
	})

	file, err := os.OpenFile("/data/postgres/pg_hba.conf", os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		str := fmt.Sprintf("%s %s %s %s %s\n", entry.Type, entry.Database, entry.User, entry.Address, entry.Method)
		_, err := file.Write([]byte(str))
		if err != nil {
			return err
		}
	}

	return nil
}

func createRequiredUsers() error {
	node, err := flypg.NewNode()
	if err != nil {
		return err
	}

	conn, err := node.NewLocalConnection(context.TODO())
	if err != nil {
		return err
	}

	curUsers, err := admin.ListUsers(context.TODO(), conn)
	if err != nil {
		return errors.Wrap(err, "failed to list current users")
	}

	credMap := map[string]string{
		"flypgadmin": os.Getenv("SU_PASSWORD"),
	}

	for user, pass := range credMap {
		exists := false
		for _, curUser := range curUsers {
			if user == curUser.Username {
				exists = true
			}
		}
		var sql string

		if exists {
			sql = fmt.Sprintf("ALTER USER %s WITH PASSWORD '%s'", user, pass)
		} else {
			// create user
			switch user {
			case "flypgadmin":
				fmt.Println("Creating flypgadmin")
				sql = fmt.Sprintf(`CREATE USER %s WITH SUPERUSER LOGIN PASSWORD '%s'`, user, pass)
			}
			_, err := conn.Exec(context.Background(), sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func memTotal() (memoryMb int64, err error) {
	if raw := os.Getenv("FLY_VM_MEMORY_MB"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, err
		}
		memoryMb = parsed
	}

	if memoryMb == 0 {
		v, err := mem.VirtualMemory()
		if err != nil {
			return 0, err
		}
		memoryMb = int64(v.Total / 1024 / 1024)
	}

	return
}

func max(n ...int64) (max int64) {
	for _, num := range n {
		if num > max {
			max = num
		}
	}
	return
}

func PanicHandler(err error) {
	debug := os.Getenv("DEBUG")
	if debug != "" {
		fmt.Println(err.Error())
		fmt.Println("Entering debug mode... (Timeout: 10 minutes")
		time.Sleep(time.Minute * 10)
	}
	panic(err)
}
