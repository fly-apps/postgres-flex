package state

import (
	"encoding/json"
	"errors"

	"github.com/hashicorp/consul/api"
)

type Cluster struct {
	Members []*Member `json:"members"`
	client  ConsulClient
}

type Member struct {
	ID       int32  `json:"id"`
	Hostname string `json:"hostname"`
	Region   string `json:"region"`
	Primary  bool   `json:"primary"`
}

const ClusterKey string = "Cluster"

// ErrCAS represents a check-and-set error
var ErrCAS = errors.New("Key has changed since we last read it. Operation needs to be retried")

func RegisterMember(consul *ConsulClient, id int32, hostname string, region string, primary bool) error {
	cluster, modifyIndex, err := clusterState(consul)
	if err != nil {
		return err
	}

	cluster.Members = append(cluster.Members, &Member{
		ID:       id,
		Hostname: hostname,
		Region:   region,
		Primary:  primary,
	})

	if err := updateClusterState(consul, modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			RegisterMember(consul, id, hostname, region, primary)
		}
	}

	return nil
}

func UnregisterMember(consul *ConsulClient, id int32) error {
	cluster, modifyIndex, err := clusterState(consul)
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

	if err := updateClusterState(consul, modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			UnregisterMember(consul, id)
		}
	}

	return nil
}

func AssignPrimary(consul *ConsulClient, id int32) error {
	cluster, modifyIndex, err := clusterState(consul)
	if err != nil {
		return err
	}

	primaryAssigned := false

	for _, member := range cluster.Members {
		if member.ID == id {
			member.Primary = true
			primaryAssigned = true
			break
		}
	}

	if !primaryAssigned {
		// TODO - Throw error
	}

	if err := updateClusterState(consul, modifyIndex, cluster); err != nil {
		if errors.Is(err, ErrCAS) {
			AssignPrimary(consul, id)
		}
	}

	return nil
}

func CurrentPrimary(client *ConsulClient) (*Member, error) {
	cluster, _, err := clusterState(client)
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

func FindMember(consul *ConsulClient, id int32) (*Member, error) {
	cluster, _, err := clusterState(consul)
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

func clusterState(consul *ConsulClient) (*Cluster, uint64, error) {
	var (
		cluster Cluster
		key     = consul.targetKey(ClusterKey)
	)

	result, _, err := consul.client.KV().Get(key, nil)
	if err != nil {
		return nil, 0, err
	}

	if result == nil {
		return &Cluster{}, 0, nil
	}

	if err := json.Unmarshal(result.Value, &cluster); err != nil {
		return nil, 0, err
	}

	return &cluster, result.ModifyIndex, nil
}

func updateClusterState(consul *ConsulClient, modifyIndex uint64, c *Cluster) error {
	clusterJSON, err := json.Marshal(c)
	if err != nil {
		return err
	}

	kv := &api.KVPair{
		Key:         consul.targetKey(ClusterKey),
		Value:       clusterJSON,
		ModifyIndex: modifyIndex,
	}
	succ, _, err := consul.client.KV().CAS(kv, nil)
	if err != nil {
		return err
	}

	if !succ {
		return ErrCAS
	}

	return nil
}
