package admin

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

func ResolveRole(ctx context.Context, pg *pgx.Conn) (string, error) {
	var readonly string
	err := pg.QueryRow(ctx, "SHOW transaction_read_only").Scan(&readonly)
	if err != nil {
		return "offline", err
	}

	if readonly == "on" {
		return "standby", nil
	}
	return "leader", nil
}

func GrantAccess(ctx context.Context, pg *pgx.Conn, username string) error {
	sql := fmt.Sprintf("GRANT pg_read_all_data, pg_write_all_data TO %q", username)

	_, err := pg.Exec(ctx, sql)
	return err
}

func GrantSuperuser(ctx context.Context, pg *pgx.Conn, username string) error {
	sql := fmt.Sprintf("ALTER USER %s WITH SUPERUSER;", username)

	_, err := pg.Exec(ctx, sql)
	return err
}

func CreateUser(ctx context.Context, pg *pgx.Conn, username string, password string) error {
	sql := fmt.Sprintf(`CREATE USER %s WITH LOGIN PASSWORD '%s'`, username, password)

	_, err := pg.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

func ChangePassword(ctx context.Context, pg *pgx.Conn, username, password string) error {
	sql := fmt.Sprintf("ALTER USER %s WITH LOGIN PASSWORD '%s';", username, password)

	_, err := pg.Exec(ctx, sql)
	return err
}

func CreateDatabase(ctx context.Context, pg *pgx.Conn, name string) (interface{}, error) {
	databases, err := ListDatabases(ctx, pg)
	if err != nil {
		return false, err
	}

	for _, db := range databases {
		if db.Name == name {
			return true, nil
		}
	}

	sql := fmt.Sprintf("CREATE DATABASE %s OWNER %s;", name, name)
	_, err = pg.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	return true, nil
}

func DeleteDatabase(ctx context.Context, pg *pgx.Conn, name string) error {
	sql := fmt.Sprintf("DROP DATABASE %s;", name)

	_, err := pg.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

func EnableExtension(ctx context.Context, pg *pgx.Conn, extension string) error {
	sql := fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s;", extension)
	_, err := pg.Exec(context.Background(), sql)
	return err
}

func ListDatabases(ctx context.Context, pg *pgx.Conn) ([]DbInfo, error) {
	sql := `
		SELECT d.datname,
					(SELECT array_agg(u.usename::text order by u.usename) 
						from pg_user u 
						where has_database_privilege(u.usename, d.datname, 'CONNECT')) as allowed_users
		from pg_database d where d.datistemplate = false
		order by d.datname;
		`

	rows, err := pg.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := []DbInfo{}

	for rows.Next() {
		di := DbInfo{}
		if err := rows.Scan(&di.Name, &di.Users); err != nil {
			return nil, err
		}
		values = append(values, di)
	}

	return values, nil
}

func FindDatabase(ctx context.Context, pg *pgx.Conn, name string) (*DbInfo, error) {
	sql := `
	SELECT 
		datname, 
		(SELECT array_agg(u.usename::text order by u.usename) FROM pg_user u WHERE has_database_privilege(u.usename, d.datname, 'CONNECT')) as allowed_users 
	FROM pg_database d WHERE d.datname='%s';
	`

	sql = fmt.Sprintf(sql, name)

	row := pg.QueryRow(ctx, sql)

	db := new(DbInfo)
	if err := row.Scan(&db.Name, &db.Users); err != nil {
		return nil, err
	}

	return db, nil
}

type UserInfo struct {
	Username  string   `json:"username"`
	SuperUser bool     `json:"superuser"`
	Databases []string `json:"databases"`
}

type DbInfo struct {
	Name  string   `json:"name"`
	Users []string `json:"users"`
}

func ListUsers(ctx context.Context, pg *pgx.Conn) ([]UserInfo, error) {
	sql := `
		select u.usename,
			usesuper as superuser,
      (select array_agg(d.datname::text order by d.datname)
				from pg_database d
				WHERE datistemplate = false
				AND has_database_privilege(u.usename, d.datname, 'CONNECT')
			) as allowed_databases
			from pg_user u
			join pg_authid a on u.usesysid = a.oid
			order by u.usename
			`

	rows, err := pg.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := []UserInfo{}

	for rows.Next() {
		ui := UserInfo{}
		if err := rows.Scan(&ui.Username, &ui.SuperUser, &ui.Databases); err != nil {
			return nil, err
		}
		values = append(values, ui)
	}

	return values, nil
}

func FindUser(ctx context.Context, pg *pgx.Conn, username string) (*UserInfo, error) {
	users, err := ListUsers(ctx, pg)
	if err != nil {
		return nil, err
	}

	for _, u := range users {
		if u.Username == username {
			return &u, nil
		}
	}

	return nil, nil
}

func DropRole(ctx context.Context, conn *pgx.Conn, username string) error {
	sql := fmt.Sprintf("DROP ROLE %s", username)
	_, err := conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

func ReassignOwnership(ctx context.Context, conn *pgx.Conn, user, targetUser string) error {
	sql := fmt.Sprintf("REASSIGN OWNED BY %s TO %s;", user, targetUser)
	_, err := conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

func DropOwned(ctx context.Context, conn *pgx.Conn, user string) error {
	sql := fmt.Sprintf("DROP OWNED BY %s;", user)
	_, err := conn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}
