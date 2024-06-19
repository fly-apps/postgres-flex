package state

import (
	"fmt"
	"net/url"
	"os"

	"github.com/hashicorp/consul/api"
)

type Store struct {
	Client *api.Client
	prefix string
}

func NewStore() (*Store, error) {
	conf, err := clientConfig()
	if err != nil {
		return nil, err
	}

	client, err := api.NewClient(conf)
	if err != nil {
		return nil, err
	}

	prefix, err := pathPrefix()
	if err != nil {
		return nil, err
	}

	return &Store{
		Client: client,
		prefix: prefix,
	}, nil
}

func (c *Store) SetInitializationFlag() error {
	kv := &api.KVPair{Key: c.targetKey("INITIALIZED"), Value: []byte("true")}
	_, err := c.Client.KV().Put(kv, nil)
	return err
}

func (c *Store) IsInitializationFlagSet() (bool, error) {
	result, _, err := c.Client.KV().Get(c.targetKey("INITIALIZED"), nil)
	if err != nil {
		return false, err
	}

	if result == nil {
		return false, nil
	}

	return true, nil
}

func (c *Store) PushUserConfig(key string, config []byte) error {
	kv := &api.KVPair{Key: c.targetKey(key), Value: config}
	_, err := c.Client.KV().Put(kv, nil)
	return err
}

func (c *Store) PullUserConfig(key string) ([]byte, error) {
	pair, _, err := c.Client.KV().Get(c.targetKey(key), nil)
	if err != nil {
		return nil, err
	}

	if pair == nil {
		return nil, nil
	}

	return pair.Value, nil
}

func (c *Store) targetKey(key string) string {
	return c.prefix + key
}

func clientConfig() (*api.Config, error) {
	u, err := url.Parse(resolveEndpoint())
	if err != nil {
		panic(err)
	}

	token, set := u.User.Password()
	if !set {
		return nil, fmt.Errorf("consul token not set")
	}

	u.User = nil

	return &api.Config{
		Token:   token,
		Scheme:  u.Scheme,
		Address: u.Host,
	}, nil
}

func pathPrefix() (string, error) {
	u, err := url.Parse(resolveEndpoint())
	if err != nil {
		return "", err
	}

	return u.Path[1:], nil
}

func resolveEndpoint() string {
	consulURL := os.Getenv("CONSUL_URL")
	if consulURL == "" {
		consulURL = os.Getenv("FLY_CONSUL_URL")
	}

	return consulURL
}
