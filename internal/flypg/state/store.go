// Package state manages the persistent state store for the flypg cluster.
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

// TryClaimPrimary attempts to atomically claim this cluster for the given
// machine. It replaces the old check-then-act pattern (read the flag, then
// separately decide to become primary), which raced whenever more than one
// node's Init/PostInit ran the check before the eventual primary had gotten
// around to setting the flag - both would see it unset and both would try
// to become primary.
//
// Consul's CAS with ModifyIndex: 0 only succeeds if the key does not already
// exist, so when multiple nodes call this concurrently, exactly one write
// wins across the whole cluster - Consul's Raft log serializes it, so there
// is no window between "check" and "act" for another node to slip through.
//
// Returns true if this machine is the primary: either it just won the
// claim, or it already won an earlier attempt (e.g. retrying after its own
// crash), recognized by the stored value matching its own machineID rather
// than someone else's.
func (c *Store) TryClaimPrimary(machineID string) (bool, error) {
	key := c.targetKey("INITIALIZED")

	ok, _, err := c.Client.KV().CAS(&api.KVPair{
		Key:         key,
		Value:       []byte(machineID),
		ModifyIndex: 0,
	}, nil)
	if err != nil {
		return false, fmt.Errorf("failed to claim primary: %s", err)
	}
	if ok {
		return true, nil
	}
	pair, _, err := c.Client.KV().Get(key, nil)
	if err != nil {
		return false, fmt.Errorf("failed to verify primary claim: %s", err)
	}
	return pair != nil && string(pair.Value) == machineID, nil
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
