package flypg

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/fly-apps/postgres-standalone/pkg/privnet"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type Credentials struct {
	Username string
	Password string
}

type Node struct {
	AppName             string
	PrivateIP           net.IP
	DataDir             string
	SUCredentials       Credentials
	OperatorCredentials Credentials
	PGPort              int
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName: "local",
		PGPort:  5432,
		DataDir: "/data",
	}

	if appName := os.Getenv("FLY_APP_NAME"); appName != "" {
		node.AppName = appName
	}

	var err error

	node.PrivateIP, err = privnet.PrivateIPv6()
	if err != nil {
		return nil, errors.Wrap(err, "error getting private ip")
	}

	if port, err := strconv.Atoi(os.Getenv("PG_PORT")); err == nil {
		node.PGPort = port
	}

	node.SUCredentials = Credentials{
		Username: "flypgadmin",
		Password: os.Getenv("SU_PASSWORD"),
	}

	node.OperatorCredentials = Credentials{
		Username: "postgres",
		Password: os.Getenv("OPERATOR_PASSWORD"),
	}

	return node, nil
}

func (n *Node) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP.String(), strconv.Itoa(n.PGPort))
	return openConnection(ctx, host, n.OperatorCredentials)
}

func openConnection(ctx context.Context, host string, creds Credentials) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	url := fmt.Sprintf("postgres://%s/postgres", host)
	conf, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	conf.User = creds.Username
	conf.Password = creds.Password
	conf.ConnectTimeout = 5 * time.Second

	conn, err := pgx.ConnectConfig(ctx, conf)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
