package lstree

// ObjectType is an Enum for the type of object of
// the ls-tree entry, which can be can be tree, blob or commit
type ObjectType int

// Entry represents a single ls-tree entry
type Entry struct {
	Mode []byte
	Type ObjectType
	Oid  string
	Path string
}

// Entries holds every ls-tree Entry
type Entries []Entry

// Enum values for ObjectType
const (
	Tree ObjectType = iota
	Blob
	Submodule
)

func (e Entries) Len() int {
	return len(e)
}

func (e Entries) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Less sorts entries by type in the order [*tree *blobs *submodules]
func (e Entries) Less(i, j int) bool {
	return e[i].Type < e[j].Type
}
