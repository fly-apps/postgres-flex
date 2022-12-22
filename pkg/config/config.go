package config

import (
	"context"
	"github.com/fly-apps/postgres-flex/pkg/flypg/state"
	"github.com/jackc/pgx/v4"
	"io"
)

type ConfigMap map[string]interface{}

type ConfigModule interface {
	Print(io.Writer) error
	Setup() error
	WriteDefaults() error
	WriteUserConfig(context.Context, *pgx.Conn, *state.ConsulClient, ConfigMap) error
	SyncUserConfig(ctx context.Context, consul *state.ConsulClient) error
	RuntimeApply(ctx context.Context, conn *pgx.Conn) error
}
