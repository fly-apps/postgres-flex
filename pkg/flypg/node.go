package flypg

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/fly-apps/postgres-standalone/pkg/privnet"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

const (
	BackendStoreConsul      = "consul"
	BackendStoreEtcd        = "etcdv3"
	BackendStoreUnspecified = ""
)

type Credentials struct {
	Username string
	Password string
}

type Node struct {
	AppName       string
	PrivateIP     net.IP
	Region        string
	PrimaryRegion string
	DataDir       string

	SUCredentials       Credentials
	ReplCredentials     Credentials
	OperatorCredentials Credentials

	BackendStore    string
	BackendStoreURL *url.URL

	KeeperUID string
	StoreNode string

	PGPort      int
	PGProxyPort int
}

func NewNode() (*Node, error) {
	node := &Node{
		AppName:     "local",
		PGPort:      5433,
		PGProxyPort: 5432,
		Region:      "local",
		DataDir:     "/data",
	}

	if region := os.Getenv("FLY_REGION"); region != "" {
		node.Region = region
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

	return node, nil
}

func openConnection(ctx context.Context, hosts []string, mode string, creds Credentials) (*pgx.Conn, error) {
	if mode == "" {
		mode = "any"
	}

	result := make(chan *pgx.Conn, len(hosts))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for _, host := range hosts {
		url := fmt.Sprintf("postgres://%s/postgres?target_session_attrs=%s", host, mode)
		conf, err := pgx.ParseConfig(url)
		if err != nil {
			return nil, err
		}
		conf.User = creds.Username
		conf.Password = creds.Password
		conf.ConnectTimeout = 5 * time.Second

		go func() {
			if cnn, err := pgx.ConnectConfig(ctx, conf); err == nil {
				result <- cnn
			}
		}()
	}

	select {
	case cnn := <-result:
		return cnn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) NewLocalConnection(ctx context.Context) (*pgx.Conn, error) {
	host := net.JoinHostPort(n.PrivateIP.String(), strconv.Itoa(n.PGPort))
	return openConnection(ctx, []string{host}, "any", n.SUCredentials)
}
