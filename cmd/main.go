package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"time"

	"github.com/fly-examples/fly-postgres/pkg/flypg"
	"github.com/fly-examples/fly-postgres/pkg/supervisor"
)

func main() {
	if _, err := os.Stat("/data/postgres"); os.IsNotExist(err) {
		setDirOwnership()
		initializePostgres()
	}

	// TODO - This is just for dev'ing and will need to change.  Additional users will be added by users
	// and this will break things.
	if err := setDefaultHBA(); err != nil {
		PanicHandler(err)
	}

	consulClient, err := flypg.NewConsulClient()
	if err != nil {
		PanicHandler(err)
	}

	if err := consulClient.Register(); err != nil {
		PanicHandler(err)
	}

	svisor := supervisor.New("flypg", 5*time.Minute)
	svisor.AddProcess("flypg", "gosu postgres postgres -D /data/postgres/")
	err = svisor.Run()
	if err != nil {
		consulClient.ReleaseSession()
	}

}

func initializePostgres() {
	fmt.Println("Initializing Postgres")

	if err := ioutil.WriteFile("/data/.default_password", []byte(os.Getenv("PGPASSWORD")), 0644); err != nil {
		PanicHandler(err)
	}

	cmd := exec.Command("gosu", "postgres", "initdb", "--pgdata", "/data/postgres", "--pwfile=/data/.default_password")
	_, err := cmd.CombinedOutput()
	if err != nil {
		PanicHandler(err)
	}

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
	if err := os.Chown("/data", pgUID, pgGID); err != nil {
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
		User:     "postgres",
		Address:  "0.0.0.0/0",
		Method:   "md5",
	})

	entries = append(entries, HBAEntry{
		Type:     "host",
		Database: "all",
		User:     "postgres",
		Address:  "::0/0",
		Method:   "md5",
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

func PanicHandler(err error) {
	debug := os.Getenv("DEBUG")
	if debug != "" {
		fmt.Println(err.Error())
		fmt.Println("Entering debug mode... (Timeout: 10 minutes")
		time.Sleep(time.Minute * 10)
	}
	panic(err)
}
