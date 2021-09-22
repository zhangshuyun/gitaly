package catfile

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// ObjectInfo represents a header returned by `git cat-file --batch`
type ObjectInfo struct {
	Oid  git.ObjectID
	Type string
	Size int64
}

// NotFoundError is returned when requesting an object that does not exist.
type NotFoundError struct{ error }

// IsNotFound tests whether err has type NotFoundError.
func IsNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

// IsBlob returns true if object type is "blob"
func (o *ObjectInfo) IsBlob() bool {
	return o.Type == "blob"
}

// ParseObjectInfo reads from a reader and parses the data into an ObjectInfo struct
func ParseObjectInfo(stdout *bufio.Reader) (*ObjectInfo, error) {
	infoLine, err := stdout.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read info line: %w", err)
	}

	infoLine = strings.TrimSuffix(infoLine, "\n")
	if strings.HasSuffix(infoLine, " missing") {
		return nil, NotFoundError{fmt.Errorf("object not found")}
	}

	info := strings.Split(infoLine, " ")
	if len(info) != 3 {
		return nil, fmt.Errorf("invalid info line: %q", infoLine)
	}

	oid, err := git.NewObjectIDFromHex(info[0])
	if err != nil {
		return nil, fmt.Errorf("parse object ID: %w", err)
	}

	objectSize, err := strconv.ParseInt(info[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse object size: %w", err)
	}

	return &ObjectInfo{
		Oid:  oid,
		Type: info[1],
		Size: objectSize,
	}, nil
}
