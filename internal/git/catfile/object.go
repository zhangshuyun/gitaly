package catfile

import (
	"io"
)

// Object represents data returned by `git cat-file --batch`
type Object struct {
	// ObjectInfo represents main information about object
	ObjectInfo
	// Reader provides raw data about object. It differs for each type of object(tag, commit, tree, log, etc.)
	io.Reader
}
