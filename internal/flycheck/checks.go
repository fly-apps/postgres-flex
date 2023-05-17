package flycheck

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/superfly/fly-checks/check"
)

const Port = 5500

func Handler() http.Handler {
	r := http.NewServeMux()

	if os.Getenv("IS_BARMAN") != "" {
		r.HandleFunc("/flycheck/vm", runVMChecks)
		r.HandleFunc("/flycheck/connection", runBarmanConnectionChecks)
		r.HandleFunc("/flycheck/role", runBarmanRoleCheck)
		return r
	}

	r.HandleFunc("/flycheck/vm", runVMChecks)
	r.HandleFunc("/flycheck/pg", runPGChecks)
	r.HandleFunc("/flycheck/role", runRoleCheck)

	return r
}

func runVMChecks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), (5 * time.Second))
	defer cancel()

	suite := &check.CheckSuite{Name: "VM"}
	suite = CheckVM(suite)

	go func(ctx context.Context) {
		suite.Process(ctx)
		cancel()
	}(ctx)

	<-ctx.Done()

	handleCheckResponse(w, suite, false)
}

func runPGChecks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), (5 * time.Second))
	defer cancel()

	suite := &check.CheckSuite{Name: "PG"}
	suite, err := CheckPostgreSQL(ctx, suite)
	if err != nil {
		suite.ErrOnSetup = err
		cancel()
	}

	go func() {
		suite.Process(ctx)
		cancel()
	}()

	<-ctx.Done()

	handleCheckResponse(w, suite, false)
}

func runRoleCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), (time.Second * 5))
	defer cancel()

	suite := &check.CheckSuite{Name: "Role"}
	suite, err := PostgreSQLRole(ctx, suite)
	if err != nil {
		suite.ErrOnSetup = err
		cancel()
	}

	go func() {
		suite.Process(ctx)
		cancel()
	}()

	<-ctx.Done()

	handleCheckResponse(w, suite, true)
}

func runBarmanConnectionChecks(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), (5 * time.Second))
	defer cancel()

	suite := &check.CheckSuite{Name: "Connection"}
	suite = CheckBarmanConnection(suite)

	go func(ctx context.Context) {
		suite.Process(ctx)
		cancel()
	}(ctx)

	<-ctx.Done()

	handleCheckResponse(w, suite, false)
}

func runBarmanRoleCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), (time.Second * 5))
	defer cancel()

	suite := &check.CheckSuite{Name: "Role"}
	suite.AddCheck("role", func() (string, error) {
		return "barman", nil
	})

	go func() {
		suite.Process(ctx)
		cancel()
	}()

	<-ctx.Done()

	handleCheckResponse(w, suite, true)
}

func handleCheckResponse(w http.ResponseWriter, suite *check.CheckSuite, raw bool) {
	if suite.ErrOnSetup != nil {
		handleError(w, suite.ErrOnSetup)
		return
	}
	var result string
	if raw {
		result = suite.RawResult()
	} else {
		result = suite.Result()
	}
	if !suite.Passed() {
		handleError(w, errors.New(result))
		return
	}
	if _, err := io.WriteString(w, result); err != nil {
		log.Printf("failed to handle check response: %s", err)
	}
}

func handleError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	if _, err := io.WriteString(w, err.Error()); err != nil {
		log.Printf("failed to handle check error: %s", err)
	}
}
