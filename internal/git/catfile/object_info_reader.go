package catfile

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

// ObjectInfo represents a header returned by `git cat-file --batch`
type ObjectInfo struct {
	Oid  git.ObjectID
	Type string
	Size int64
}

// IsBlob returns true if object type is "blob"
func (o *ObjectInfo) IsBlob() bool {
	return o.Type == "blob"
}

// ObjectID is the ID of the object.
func (o *ObjectInfo) ObjectID() git.ObjectID {
	return o.Oid
}

// ObjectType is the type of the object.
func (o *ObjectInfo) ObjectType() string {
	return o.Type
}

// ObjectSize is the size of the object.
func (o *ObjectInfo) ObjectSize() int64 {
	return o.Size
}

// NotFoundError is returned when requesting an object that does not exist.
type NotFoundError struct{ error }

// IsNotFound tests whether err has type NotFoundError.
func IsNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
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

// ObjectInfoReader returns information about an object referenced by a given revision.
type ObjectInfoReader interface {
	cacheable

	// Info requests information about the revision pointed to by the given revision.
	Info(context.Context, git.Revision) (*ObjectInfo, error)
}

// objectInfoReader is a reader for Git object information. This reader is implemented via a
// long-lived  `git cat-file --batch-check` process such that we do not have to spawn a separate
// process per object info we're about to read.
type objectInfoReader struct {
	cmd    *command.Command
	stdout *bufio.Reader
	sync.Mutex

	closed bool

	counter *prometheus.CounterVec
}

func newObjectInfoReader(
	ctx context.Context,
	repo git.RepositoryExecutor,
	counter *prometheus.CounterVec,
) (*objectInfoReader, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "catfile.ObjectInfoReader")

	batchCmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch-check"},
			},
		},
		git.WithStdin(command.SetupStdin),
	)
	if err != nil {
		return nil, err
	}

	objectInfoReader := &objectInfoReader{
		cmd:     batchCmd,
		stdout:  bufio.NewReader(batchCmd),
		counter: counter,
	}
	go func() {
		<-ctx.Done()
		// This is crucial to prevent leaking file descriptors.
		objectInfoReader.close()
		span.Finish()
	}()

	return objectInfoReader, nil
}

func (o *objectInfoReader) close() {
	o.Lock()
	defer o.Unlock()

	_ = o.cmd.Wait()

	o.closed = true
}

func (o *objectInfoReader) isClosed() bool {
	o.Lock()
	defer o.Unlock()
	return o.closed
}

func (o *objectInfoReader) isDirty() bool {
	// We always consume object info directly, so the reader cannot ever be dirty.
	return false
}

func (o *objectInfoReader) Info(ctx context.Context, revision git.Revision) (*ObjectInfo, error) {
	trace, finish := startTrace(ctx, o.counter, "catfile.Info")
	defer finish()

	o.Lock()
	defer o.Unlock()

	if _, err := fmt.Fprintln(o.cmd, revision.String()); err != nil {
		return nil, err
	}
	trace.recordRequest("info")

	return ParseObjectInfo(o.stdout)
}
