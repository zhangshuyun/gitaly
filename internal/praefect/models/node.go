package models

import (
	"encoding/json"
	"fmt"
)

// Node describes an address that serves a storage
type Node struct {
	Storage        string `toml:"storage"`
	Address        string `toml:"address"`
	Token          string `toml:"token"`
	DefaultPrimary bool   `toml:"primary"`
}

func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Storage string `json:"storage"`
		Address string `json:"address"`
		Primary bool   `json:"primary"`
	}{
		Storage: n.Storage,
		Address: n.Address,
		Primary: n.DefaultPrimary,
	})
}

// String prints out the node attributes but hiding the token
func (n Node) String() string {
	return fmt.Sprintf("storage_name: %s, address: %s, primary: %v", n.Storage, n.Address, n.DefaultPrimary)
}

// Repository describes a repository's relative path and its primary and list of secondaries
type Repository struct {
	RelativePath string
	Primary      Node
	Replicas     []Node
}

// Clone returns deep copy of the Repository
func (r Repository) Clone() Repository {
	clone := r
	clone.Replicas = make([]Node, len(r.Replicas))
	copy(clone.Replicas, r.Replicas)
	return clone
}
