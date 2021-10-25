package git

// ObjectInfo is an interface encapsulating information about objects.
type ObjectInfo interface {
	ObjectID() ObjectID
	ObjectType() string
	ObjectSize() int64
}
