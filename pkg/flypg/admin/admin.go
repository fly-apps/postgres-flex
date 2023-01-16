package admin

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"
)

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
	return err
}

func ChangePassword(ctx context.Context, pg *pgx.Conn, username, password string) error {
	sql := fmt.Sprintf("ALTER USER %s WITH LOGIN PASSWORD '%s';", username, password)

	_, err := pg.Exec(ctx, sql)
	return err
}

func CreateDatabaseWithOwner(ctx context.Context, pg *pgx.Conn, name, owner string) error {
	dbInfo, err := FindDatabase(ctx, pg, name)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	// Database already exists.
	if dbInfo != nil {
		return nil
	}
	sql := fmt.Sprintf("CREATE DATABASE %s OWNER %s;", name, owner)
	_, err = pg.Exec(ctx, sql)

	return err
}

func CreateDatabase(ctx context.Context, pg *pgx.Conn, name string) error {
	dbInfo, err := FindDatabase(ctx, pg, name)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	// Database already exists.
	if dbInfo != nil {
		return nil
	}

	sql := fmt.Sprintf("CREATE DATABASE %s;", name)
	_, err = pg.Exec(ctx, sql)
	return err
}

func DeleteDatabase(ctx context.Context, pg *pgx.Conn, name string) error {
	sql := fmt.Sprintf("DROP DATABASE %s;", name)

	_, err := pg.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

type ReplicationSlot struct {
	MemberID  int32
	Name      string
	Type      string
	Active    bool
	WalStatus string
}

func ListReplicationSlots(ctx context.Context, pg *pgx.Conn) ([]ReplicationSlot, error) {
	sql := fmt.Sprintf("SELECT slot_name, slot_type, active, wal_status from pg_replication_slots;")
	rows, err := pg.Query(ctx, sql)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	var slots []ReplicationSlot

	for rows.Next() {
		var slot ReplicationSlot
		if err := rows.Scan(&slot.Name, &slot.Type, &slot.Active, &slot.WalStatus); err != nil {
			return nil, err
		}

		slotArr := strings.Split(slot.Name, "_")
		// Only look at repmgr replication slots.
		if slotArr[0] == "repmgr" {
			// Resolve member id from slot name.
			idStr := slotArr[2]

			num, err := strconv.ParseInt(idStr, 10, 32)
			if err != nil {
				fmt.Printf("failed to parse member id %s", idStr)
				continue
			}

			slot.MemberID = int32(num)
			slots = append(slots, slot)
		}
	}

	return slots, nil
}

func DropReplicationSlot(ctx context.Context, pg *pgx.Conn, name string) error {
	sql := fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", name)

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

func SetConfigurationSetting(ctx context.Context, conn *pgx.Conn, key string, value interface{}) error {
	sql := fmt.Sprintf("SET %s to %s", key, value)
	_, err := conn.Exec(ctx, sql)
	return err
}

func ReloadPostgresConfig(ctx context.Context, pg *pgx.Conn) error {
	sql := "SELECT pg_reload_conf()"

	_, err := pg.Exec(ctx, sql)
	return err
}

func SettingExists(ctx context.Context, pg *pgx.Conn, setting string) (bool, error) {
	sql := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_settings WHERE name='%s')", setting)
	var out bool
	if err := pg.QueryRow(ctx, sql).Scan(&out); err != nil {
		return false, err
	}
	return out, nil
}

func SettingRequiresRestart(ctx context.Context, pg *pgx.Conn, setting string) (bool, error) {
	sql := fmt.Sprintf("SELECT pending_restart FROM pg_settings WHERE name='%s'", setting)
	row := pg.QueryRow(ctx, sql)
	var out bool
	if err := row.Scan(&out); err != nil {
		return false, err
	}
	return out, nil
}

type PGSetting struct {
	Name           string    `json:"name,omitempty"`
	Setting        string    `json:"setting,omitempty"`
	VarType        *string   `json:"vartype,omitempty"`
	MinVal         *string   `json:"min_val,omitempty"`
	MaxVal         *string   `json:"max_val,omitempty"`
	EnumVals       *[]string `json:"enumvals,omitempty"`
	Context        *string   `json:"context,omitempty"`
	Unit           *string   `json:"unit,omitempty"`
	Desc           *string   `json:"short_desc,omitempty"`
	PendingChange  *string   `json:"pending_change,omitempty"`
	PendingRestart *bool     `json:"pending_restart,omitempty"`
}

func GetSetting(ctx context.Context, pg *pgx.Conn, setting string) (*PGSetting, error) {
	sql := fmt.Sprintf("SELECT name, setting, vartype, min_val, max_val, enumvals, context, unit, short_desc, pending_restart FROM pg_settings WHERE name='%s'", setting)
	row := pg.QueryRow(ctx, sql)
	out := PGSetting{}
	if err := row.Scan(&out.Name, &out.Setting, &out.VarType, &out.MinVal, &out.MaxVal, &out.EnumVals, &out.Context, &out.Unit, &out.Desc, &out.PendingRestart); err != nil {
		return nil, err
	}
	return &out, nil
}
