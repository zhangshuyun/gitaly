package models

type StorageNode struct {
	ID          int
	StorageName string
	Address     string
}

type Shard struct {
	RelativePath string
	Primary      StorageNode
	Secondaries  []StorageNode
}
