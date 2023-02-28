package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"log"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
)

func renderJSON(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("failed to write json response: %s", err)
	}
}

func renderErr(w http.ResponseWriter, err error) {
	renderJSON(w, errRes{Error: err.Error()}, status(err))
}

func status(err error) int {
	if err == nil {
		return http.StatusOK
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return http.StatusNotFound
	}

	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42710": // unique violation
			return http.StatusConflict
		case "23505": // unique violation
			return http.StatusConflict
		case "23503": // foreign key violation
			return http.StatusBadRequest
		case "23502": // not null violation
			return http.StatusBadRequest
		default:
			return http.StatusInternalServerError
		}
	}
	return http.StatusInternalServerError
}
