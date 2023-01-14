package state

import (
	"encoding/json"

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

func RegisterMember(consul *ConsulClient, id int32, hostname string, region string, primary bool) error {
	cluster, err := clusterState(consul)
	if err != nil {
		return err
	}

	member := &Member{
		ID:       id,
		Hostname: hostname,
		Region:   region,
		Primary:  primary,
	}

	cluster.Members = append(cluster.Members, member)

	return updateClusterState(consul, cluster)
}

func UnregisterMember(consul *ConsulClient, id int32) error {
	cluster, err := clusterState(consul)
	if err != nil {
		return err
	}

	var members []*Member

	for _, member := range cluster.Members {
		if member.ID != id {
			members = append(members, member)
		}
	}

	cluster.Members = members

	return updateClusterState(consul, cluster)
}

func AssignPrimary(client *ConsulClient, id int32) error {
	cluster, err := clusterState(client)
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

	return updateClusterState(client, cluster)
}

func CurrentPrimary(client *ConsulClient) (*Member, error) {
	cluster, err := clusterState(client)
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
	cluster, err := clusterState(consul)
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

func clusterState(consul *ConsulClient) (*Cluster, error) {
	var (
		cluster Cluster
		key     = consul.targetKey(ClusterKey)
	)

	result, _, err := consul.client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}

	if result == nil {
		return &Cluster{}, nil
	}

	if err := json.Unmarshal(result.Value, &cluster); err != nil {
		return nil, err
	}

	return &cluster, nil
}

func updateClusterState(consul *ConsulClient, c *Cluster) error {
	clusterJSON, err := json.Marshal(c)
	if err != nil {
		return err
	}

	kv := &api.KVPair{Key: consul.targetKey(ClusterKey), Value: clusterJSON}
	_, err = consul.client.KV().Put(kv, nil)
	if err != nil {
		return err
	}

	return nil
}
