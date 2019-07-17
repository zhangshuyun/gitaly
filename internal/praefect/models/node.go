package models

// StorageNode describes an address that serves a storage
type StorageNode struct {
	ID      int
	Storage string `toml:"storage"`
	Address string `toml:"address"`
	Token   string `toml:"token"`
}

// Repository describes a repository's relative path and its primary and list of secondaries
type Repository struct {
	ID           int
	RelativePath string
	Primary      StorageNode
	Replicas     []StorageNode
}
