package models

// StorageNode describes an address that serves a storage
type StorageNode struct {
	ID      int
	Storage string `toml:"storage"`
	Address string `toml:"address"`
	Token   string `toml:"token"`
}

// Shard describes a repository's relative path and its primary and list of secondaries
type Shard struct {
	ID           int
	Storage      string
	RelativePath string
	Primary      StorageNode
	Secondaries  []StorageNode
}
