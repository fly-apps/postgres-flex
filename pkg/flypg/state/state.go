package state

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/hashicorp/consul/api"
)

type consulClient struct {
	client *api.Client
	prefix string
}

func NewConsulClient() (*consulClient, error) {
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

	return &consulClient{
		client: client,
		prefix: prefix,
	}, nil
}

func (c *consulClient) CurrentPrimary() (string, error) {
	pair, _, err := c.client.KV().Get(c.targetKey("PRIMARY_NODE"), nil)
	if err != nil {
		return "", err
	}

	if pair != nil {
		return string(pair.Value), nil
	}

	return "", nil
}

func (c *consulClient) RegisterPrimary(hostname string) error {
	kv := &api.KVPair{Key: c.targetKey("PRIMARY_NODE"), Value: []byte(hostname)}
	_, err := c.client.KV().Put(kv, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *consulClient) Node(id int32) (*api.KVPair, error) {
	idBytes, err := json.Marshal(id)
	if err != nil {
		return nil, err
	}

	key := c.targetKey(string(idBytes))
	result, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *consulClient) RegisterNode(id int32, hostname string) error {
	node, err := c.Node(id)
	if err != nil {
		return err
	}

	if node != nil {
		return nil
	}

	idBytes, err := json.Marshal(id)
	if err != nil {
		return err
	}

	kv := &api.KVPair{Key: c.targetKey(string(idBytes)), Value: []byte(hostname)}
	_, err = c.client.KV().Put(kv, nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *consulClient) DeleteNode(id int32) error {
	idBytes, err := json.Marshal(id)
	if err != nil {
		return err
	}

	_, err = c.client.KV().Delete(string(idBytes), nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *consulClient) targetKey(key string) string {
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
