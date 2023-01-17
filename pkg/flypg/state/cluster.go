package state

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hashicorp/consul/api"
)

type ClusterState struct {
	Store *Store
}

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

func NewClusterState() (*ClusterState, error) {
	store, err := NewStore()
	if err != nil {
		return nil, err
	}

	return &ClusterState{
		Store: store,
	}, nil
}

func (c *ClusterState) RegisterMember(id int32, hostname string, region string, primary bool) error {
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

		return err
	}

	return nil
}

func (c *ClusterState) UnregisterMember(id int32) error {
	cluster, modifyIndex, err := c.clusterData()
	if err != nil {
		return err
	}

	// Rebuild member slice without the target member
	exists := false
	var members []*Member
	for _, member := range cluster.Members {
		if member.ID == id {
			exists = true
			continue
		}

		members = append(members, member)
	}

	if !exists {
		return nil
	}

	cluster.Members = members

	if err := c.updateClusterState(modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			c.UnregisterMember(id)
		}

		return err
	}

	return nil
}

func (c *ClusterState) AssignPrimary(id int32) error {
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

		return err
	}

	return nil
}

func (c *ClusterState) PrimaryMember() (*Member, error) {
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

func (c *ClusterState) FindMemberByID(id int32) (*Member, error) {
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

func (c *ClusterState) FindMemberByHostname(hostname string) (*Member, error) {
	cluster, _, err := c.clusterData()
	if err != nil {
		return nil, err
	}

	for _, member := range cluster.Members {
		if member.Hostname == hostname {
			return member, nil
		}
	}

	return nil, nil
}

func (c *ClusterState) clusterData() (*ClusterData, uint64, error) {
	var (
		cluster ClusterData
		key     = c.Store.targetKey(stateKey)
	)

	result, _, err := c.Store.Client.KV().Get(key, nil)
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

func (c *ClusterState) updateClusterState(modifyIndex uint64, cd *ClusterData) error {
	clusterJSON, err := json.Marshal(cd)
	if err != nil {
		return err
	}

	kv := &api.KVPair{
		Key:         c.Store.targetKey(stateKey),
		Value:       clusterJSON,
		ModifyIndex: modifyIndex,
	}
	succ, _, err := c.Store.Client.KV().CAS(kv, nil)
	if err != nil {
		return err
	}

	if !succ {
		fmt.Println(ErrCAS.Error())
		return ErrCAS
	}

	return nil
}
