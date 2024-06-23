package flypg

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/fly-apps/postgres-flex/internal/utils"
	"github.com/jackc/pgx/v5"
)

const collationVersionFile = "/data/.collationVersion"

func calculateLocaleVersionHash() (string, error) {
	output, err := utils.RunCommand("locale --version", "postgres")
	if err != nil {
		return "", fmt.Errorf("failed to read locale version: %w", err)
	}

	hash := sha256.Sum256(output)
	return hex.EncodeToString(hash[:]), nil
}

func writeCollationVersionFile(versionHash string) error {
	// Write the collation lock file.
	if err := os.WriteFile(collationVersionFile, []byte(versionHash), 0600); err != nil {
		return fmt.Errorf("failed to write collation version file: %w", err)
	}

	return nil
}

func collationHashChanged(versionHash string) (bool, error) {
	// Short-circuit if there's no collation file.
	_, err := os.Stat(collationVersionFile)
	switch {
	case os.IsNotExist(err):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("failed to stat collation lock file: %w", err)
	}

	// Read the collation version file.
	oldVersionHash, err := os.ReadFile(collationVersionFile)
	if err != nil {
		return false, fmt.Errorf("failed to read collation lock file: %w", err)
	}

	// Compare the version hashes.
	return versionHash != string(oldVersionHash), nil
}

const identifyImpactedCollationObjectsSQL = `
SELECT pg_describe_object(refclassid, refobjid, refobjsubid) AS "Collation",
	   pg_describe_object(classid, objid, objsubid) AS "Object"
FROM pg_depend d JOIN pg_collation c
	 ON refclassid = 'pg_collation'::regclass AND refobjid = c.oid
WHERE c.collversion <> pg_collation_actual_version(c.oid)
ORDER BY 1, 2;
`

type collationObject struct {
	collation string
	object    string
}

func impactedCollationObjects(ctx context.Context, conn *pgx.Conn) ([]collationObject, error) {
	rows, err := conn.Query(ctx, identifyImpactedCollationObjectsSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query impacted objects: %v", err)
	}
	defer rows.Close()

	var objects []collationObject

	var collation, object string
	for rows.Next() {
		if err := rows.Scan(&collation, &object); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		objects = append(objects, collationObject{collation: collation, object: object})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %v", err)
	}

	return objects, nil
}

func refreshCollations(ctx context.Context, dbConn *pgx.Conn, dbName string) error {
	if dbName != "template1" {
		if err := refreshDatabaseCollations(ctx, dbConn, dbName); err != nil {
			return err
		}
	}

	return refreshDatabase(ctx, dbConn, dbName)
}

func refreshDatabaseCollations(ctx context.Context, dbConn *pgx.Conn, dbName string) error {
	collations, err := fetchCollations(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("failed to fetch collations: %w", err)
	}

	for _, collation := range collations {
		if err := refreshCollation(ctx, dbConn, collation); err != nil {
			log.Printf("[WARN] failed to refresh collation version in db %s: %v\n", dbName, err)
		}
	}

	return nil
}

func refreshCollation(ctx context.Context, dbConn *pgx.Conn, collation string) error {
	query := fmt.Sprintf("ALTER COLLATION pg_catalog.\"%s\" REFRESH VERSION;", collation)
	_, err := dbConn.Exec(ctx, query)
	return err
}

func refreshDatabase(ctx context.Context, dbConn *pgx.Conn, dbName string) error {
	query := fmt.Sprintf("ALTER DATABASE %s REFRESH COLLATION VERSION;", dbName)
	_, err := dbConn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to refresh database collation version: %w", err)
	}
	return nil
}

func fetchCollations(ctx context.Context, dbConn *pgx.Conn) ([]string, error) {
	query := "SELECT DISTINCT datcollate FROM pg_database WHERE datcollate != 'C'"
	rows, err := dbConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch collations: %w", err)
	}
	defer rows.Close()

	var collations []string
	for rows.Next() {
		var collation sql.NullString
		if err := rows.Scan(&collation); err != nil {
			return nil, fmt.Errorf("failed to scan collation row: %w", err)
		}
		if collation.Valid {
			collations = append(collations, collation.String)
		}
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return collations, nil
}
