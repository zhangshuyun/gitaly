package models

// StorageNode describes an address that serves a storage
type StorageNode struct {
	ID          int
	StorageName string
	Address     string
}

// Shard describes a repository's relative path and its primary and list of secondaries
type Shard struct {
	RelativePath string
	Primary      StorageNode
	Secondaries  []StorageNode
}
