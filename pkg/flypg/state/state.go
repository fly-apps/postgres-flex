package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/hashicorp/consul/api"
)

type ClusterData struct {
	Members []*Member `json:"members"`
}

type Member struct {
	ID       int32  `json:"id"`
	Hostname string `json:"hostname"`
	Region   string `json:"region"`
	Primary  bool   `json:"primary"`
}

const stateKey string = "Cluster"

var (
	// ErrCAS represents a check-and-set error
	ErrCAS = errors.New("cluster state has changed and state update will be retried")
	// ErrMemberNotFound indicates that the target member is not currently registered in Consul
	ErrMemberNotFound = errors.New("member not found")
)

type Store struct {
	client *api.Client
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
		client: client,
		prefix: prefix,
	}, nil
}

func (c *Store) RegisterMember(id int32, hostname string, region string, primary bool) error {
	cluster, modifyIndex, err := c.clusterData()
	if err != nil {
		return err
	}

	// Short circuit if we are already registered.
	for _, members := range cluster.Members {
		if members.ID == id {
			return nil
		}
	}

	cluster.Members = append(cluster.Members, &Member{
		ID:       id,
		Hostname: hostname,
		Region:   region,
		Primary:  primary,
	})

	if err := c.updateClusterState(modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			c.RegisterMember(id, hostname, region, primary)
		}
	}

	return nil
}

func (c *Store) UnregisterMember(id int32) error {
	cluster, modifyIndex, err := c.clusterData()
	if err != nil {
		return err
	}

	// Rebuild the members slice and exclude the target member.
	var members []*Member
	for _, member := range cluster.Members {
		if member.ID != id {
			members = append(members, member)
		}
	}

	cluster.Members = members

	if err := c.updateClusterState(modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			c.UnregisterMember(id)
		}
	}

	return nil
}

func (c *Store) AssignPrimary(id int32) error {
	cluster, modifyIndex, err := c.clusterData()
	if err != nil {
		return err
	}

	primaryAssigned := false

	for _, member := range cluster.Members {
		if member.ID == id {
			primaryAssigned = true
			member.Primary = true
			continue
		}
		member.Primary = false
	}

	if !primaryAssigned {
		return ErrMemberNotFound
	}

	if err := c.updateClusterState(modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			c.AssignPrimary(id)
		}
	}

	return nil
}

func (c *Store) PrimaryMember() (*Member, error) {
	cluster, _, err := c.clusterData()
	if err != nil {
		return nil, err
	}

	for _, member := range cluster.Members {
		if member.Primary {
			return member, nil
		}
	}

	return nil, nil
}

func (c *Store) FindMember(id int32) (*Member, error) {
	cluster, _, err := c.clusterData()
	if err != nil {
		return nil, err
	}

	for _, member := range cluster.Members {
		if member.ID == id {
			return member, nil
		}
	}

	return nil, nil
}

func (c *Store) clusterData() (*ClusterData, uint64, error) {
	var (
		cluster ClusterData
		key     = c.targetKey(stateKey)
	)

	result, _, err := c.client.KV().Get(key, nil)
	if err != nil {
		return nil, 0, err
	}

	if result == nil {
		return &ClusterData{}, 0, nil
	}

	if err := json.Unmarshal(result.Value, &cluster); err != nil {
		return nil, 0, err
	}

	return &cluster, result.ModifyIndex, nil
}

func (c *Store) updateClusterState(modifyIndex uint64, cluster *ClusterData) error {
	clusterJSON, err := json.Marshal(c)
	if err != nil {
		return err
	}

	kv := &api.KVPair{
		Key:         c.targetKey(stateKey),
		Value:       clusterJSON,
		ModifyIndex: modifyIndex,
	}
	succ, _, err := c.client.KV().CAS(kv, nil)
	if err != nil {
		return err
	}

	if !succ {
		fmt.Println(ErrCAS.Error())
		return ErrCAS
	}

	return nil
}

func (c *Store) PushUserConfig(key string, config []byte) error {
	kv := &api.KVPair{Key: c.targetKey(key), Value: config}
	_, err := c.client.KV().Put(kv, nil)
	return err
}

func (c *Store) PullUserConfig(key string) ([]byte, error) {
	pair, _, err := c.client.KV().Get(c.targetKey(key), nil)
	if err != nil {
		return nil, err
	}
	return pair.Value, nil
}

func (c *Store) targetKey(key string) string {
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
