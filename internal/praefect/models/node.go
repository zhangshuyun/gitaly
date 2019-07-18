package models

type Storage struct {
	Name string
}

type Node struct {
	Address string
}

type StorageNode struct {
	ID int
	Name string
	Address string
}

type Repository struct {
	RelativePath string
	Primary *StorageNode
	Secondaries []*StorageNode
}