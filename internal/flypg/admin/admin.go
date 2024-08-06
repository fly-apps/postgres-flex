package admin

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

type Credential struct {
	Username string
	Password string
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
	return err
}

func ManageDefaultUsers(ctx context.Context, conn *pgx.Conn, creds []Credential) error {
	curUsers, err := ListUsers(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to list existing users: %s", err)
	}

	for _, c := range creds {
		exists := false
		for _, curUser := range curUsers {
			if c.Username == curUser.Username {
				exists = true
			}
		}

		if exists {
			if err := ChangePassword(ctx, conn, c.Username, c.Password); err != nil {
				return fmt.Errorf("failed to update credentials for user %s: %s", c.Username, err)
			}
		} else {
			if err := CreateUser(ctx, conn, c.Username, c.Password); err != nil {
				return fmt.Errorf("failed to create require user %s: %s", c.Username, err)
			}

			if err := GrantSuperuser(ctx, conn, c.Username); err != nil {
				return fmt.Errorf("failed to grant superuser privileges to user %s: %s", c.Username, err)
			}
		}
	}

	return nil
}

func ChangePassword(ctx context.Context, pg *pgx.Conn, username, password string) error {
	sql := fmt.Sprintf("ALTER USER %s WITH LOGIN PASSWORD '%s';", username, password)

	_, err := pg.Exec(ctx, sql)
	return err
}

func CreateDatabaseWithOwner(ctx context.Context, pg *pgx.Conn, name, owner string) error {
	dbInfo, err := FindDatabase(ctx, pg, name)
	if err != nil {
		return err
	}

	if dbInfo != nil {
		return nil
	}

	sql := fmt.Sprintf("CREATE DATABASE %s OWNER %s;", name, owner)
	_, err = pg.Exec(ctx, sql)

	return err
}

func CreateDatabase(ctx context.Context, pg *pgx.Conn, name string) error {
	dbInfo, err := FindDatabase(ctx, pg, name)
	if err != nil {
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

// GrantCreateOnPublic re-enables the public schema for normal users.
// We should look into creating better isolation in the future.
func GrantCreateOnPublic(ctx context.Context, pg *pgx.Conn) error {
	sql := "GRANT CREATE on SCHEMA PUBLIC to PUBLIC;"
	_, err := pg.Exec(ctx, sql)
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
	MemberID           int32
	Name               string
	Active             bool
	WalStatus          string
	RetainedWalInBytes int
}

func GetReplicationSlot(ctx context.Context, pg *pgx.Conn, slotName string) (*ReplicationSlot, error) {
	sql := fmt.Sprintf("SELECT slot_name, active, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal FROM pg_replication_slots where slot_name = '%s';", slotName)
	row := pg.QueryRow(ctx, sql)

	var slot ReplicationSlot
	if err := row.Scan(&slot.Name, &slot.Active, &slot.WalStatus, &slot.RetainedWalInBytes); err != nil {
		return nil, err
	}

	return &slot, nil
}

func ListReplicationSlots(ctx context.Context, pg *pgx.Conn) ([]ReplicationSlot, error) {
	sql := "SELECT slot_name, active, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal FROM pg_replication_slots;"
	rows, err := pg.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var slots []ReplicationSlot

	for rows.Next() {
		var slot ReplicationSlot
		if err := rows.Scan(&slot.Name, &slot.Active, &slot.WalStatus, &slot.RetainedWalInBytes); err != nil {
			return nil, err
		}

		// Extract the repmgr member id from the slot name.
		// Slot name has the following format: repmgr_slot_<member-id>
		slotArr := strings.Split(slot.Name, "_")
		if slotArr[0] == "repmgr" {
			idStr := slotArr[2]

			num, err := strconv.ParseInt(idStr, 10, 32)
			if err != nil {
				return nil, err
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
	_, err := pg.Exec(ctx, sql)
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

	var values []DbInfo

	for rows.Next() {
		di := DbInfo{}
		if err := rows.Scan(&di.Name, &di.Users); err != nil {
			return nil, err
		}
		values = append(values, di)
	}

	return values, nil
}

func FindDatabase(ctx context.Context, conn *pgx.Conn, name string) (*DbInfo, error) {
	dbs, err := ListDatabases(ctx, conn)
	if err != nil {
		return nil, err
	}

	for _, db := range dbs {
		if db.Name == name {
			return &db, nil
		}
	}

	return nil, nil
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

	var values []UserInfo

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

func ExtensionAvailable(ctx context.Context, pg *pgx.Conn, extension string) (bool, error) {
	sql := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name='%s')", extension)
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

func ValidatePGSettings(ctx context.Context, conn *pgx.Conn, requested map[string]interface{}) error {
	for k, v := range requested {
		exists, err := SettingExists(ctx, conn, k)
		if err != nil {
			return fmt.Errorf("failed to verify setting: %s", err)
		}
		if !exists {
			return fmt.Errorf("setting %v is not a valid config option", k)
		}

		// Verify specified extensions are installed
		if k == "shared_preload_libraries" {
			extensions := strings.Trim(v.(string), "'")
			extSlice := strings.Split(extensions, ",")
			for _, e := range extSlice {
				available, err := ExtensionAvailable(ctx, conn, e)
				if err != nil {
					return fmt.Errorf("failed to verify pg extension %s: %s", e, err)
				}

				if !available {
					return fmt.Errorf("extension %s has not been installed within this image", e)
				}
			}
		}

		if k == "max_replication_slots" {
			maxReplicationSlotsStr := v.(string)

			// Convert string to int
			maxReplicationSlots, err := strconv.ParseInt(maxReplicationSlotsStr, 10, 64)
			if err != nil {
				return fmt.Errorf("failed to parse max_replication_slots: %s", err)
			}

			slots, err := ListReplicationSlots(ctx, conn)
			if err != nil {
				return fmt.Errorf("failed to verify replication slots: %s", err)
			}

			if len(slots) > int(maxReplicationSlots) {
				return fmt.Errorf("max_replication_slots must be greater than or equal to the number of active replication slots (%d)", len(slots))
			}
		}
	}

	return nil
}
