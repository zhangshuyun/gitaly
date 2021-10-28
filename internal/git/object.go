package git

import "io"

// ObjectInfo is an interface encapsulating information about objects.
type ObjectInfo interface {
	ObjectID() ObjectID
	ObjectType() string
	ObjectSize() int64
}

// Object is an interface encapsulating an object with contents.
type Object interface {
	// ObjectInfo provides information about the object.
	ObjectInfo
	// Reader reads object data.
	io.Reader
	// WriterTo writes object data into a reader.
	io.WriterTo
}
