package packfile

import "fmt"

type ObjectType int

const (
	TUnknown ObjectType = iota
	TBlob
	TCommit
	TTree
	TTag
)

type Object struct {
	OID    string
	Type   ObjectType
	Offset uint64
}

func (o Object) String() string {
	t := "unknown"
	switch o.Type {
	case TBlob:
		t = "blob"
	case TCommit:
		t = "commit"
	case TTree:
		t = "tree"
	case TTag:
		t = "tag"
	}

	return fmt.Sprintf("%s %s\t%d", o.OID, t, o.Offset)
}
