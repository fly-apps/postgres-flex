package state

import (
	"fmt"
	"net/url"
	"os"

	"github.com/hashicorp/consul/api"
)

type ConsulClient struct {
	client *api.Client
	prefix string
}

func NewConsulClient() (*ConsulClient, error) {
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

	return &ConsulClient{
		client: client,
		prefix: prefix,
	}, nil
}

func (c *ConsulClient) PushUserConfig(key string, config []byte) error {
	kv := &api.KVPair{Key: c.targetKey(key), Value: config}
	_, err := c.client.KV().Put(kv, nil)
	return err
}

func (c *ConsulClient) PullUserConfig(key string) ([]byte, error) {
	pair, _, err := c.client.KV().Get(c.targetKey(key), nil)
	if err != nil {
		return nil, err
	}
	if pair == nil {
		return nil, nil
	}
	return pair.Value, nil
}

func (c *ConsulClient) targetKey(key string) string {
	return c.prefix + key
}

func clientConfig() (*api.Config, error) {
	u, err := url.Parse(os.Getenv("FLY_CONSUL_URL"))
	if err != nil {
		panic(err)
	}

	token, set := u.User.Password()
	if !set {
		return nil, fmt.Errorf("token not set")
	}

	u.User = nil

	return &api.Config{
		Token:   token,
		Scheme:  u.Scheme,
		Address: u.Hostname(),
	}, nil
}

func pathPrefix() (string, error) {
	u, err := url.Parse(os.Getenv("FLY_CONSUL_URL"))
	if err != nil {
		return "", err
	}

	return u.Path[1:], nil
}
