package blob

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
)

// RevlistResult is a result for the revlist pipeline step.
type RevlistResult struct {
	// Err is an error which occurred during execution of the pipeline.
	Err error

	// OID is the object ID of an object printed by git-rev-list(1).
	OID git.ObjectID
	// ObjectName is the name of the object. This is typically the path of the object if it was
	// traversed via either a tree or a commit. The path depends on the order in which objects
	// are traversed: if e.g. two different trees refer to the same blob with different names,
	// the blob's path depends on which of the trees was traversed first.
	ObjectName []byte
}

// ObjectType is a Git object type used for filtering objects.
type ObjectType string

const (
	// ObjectTypeCommit is the type of a Git commit.
	ObjectTypeCommit = ObjectType("commit")
	// ObjectTypeBlob is the type of a Git blob.
	ObjectTypeBlob = ObjectType("blob")
	// ObjectTypeTree is the type of a Git tree.
	ObjectTypeTree = ObjectType("tree")
	// ObjectTypeTag is the type of a Git tag.
	ObjectTypeTag = ObjectType("tag")
)

// revlistConfig is configuration for the revlist pipeline step.
type revlistConfig struct {
	blobLimit  int
	objectType ObjectType
}

// RevlistOption is an option for the revlist pipeline step.
type RevlistOption func(cfg *revlistConfig)

// WithBlobLimit sets up a size limit for blobs. Only blobs whose size is smaller than this limit
// will be returned by the pipeline step.
func WithBlobLimit(limit int) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.blobLimit = limit
	}
}

// WithObjectTypeFilter will set up a `--filter=object:type=` filter for git-rev-list(1). This will
// cause it to filter out any objects which do not match the given type. Because git-rev-list(1) by
// default never filters provided arguments, this option also sets up the `--filter-provided` flag.
// Note that this option is only supported starting with Git v2.32.0 or later.
func WithObjectTypeFilter(t ObjectType) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.objectType = t
	}
}

// Revlist runs git-rev-list(1) with objects and object names enabled. The returned channel will
// contain all object IDs listed by this command. Cancelling the context will cause the pipeline to
// be cancelled, too.
func Revlist(
	ctx context.Context,
	repo *localrepo.Repo,
	revisions []string,
	options ...RevlistOption,
) <-chan RevlistResult {
	var cfg revlistConfig
	for _, option := range options {
		option(&cfg)
	}

	resultChan := make(chan RevlistResult)
	go func() {
		defer close(resultChan)

		sendResult := func(result RevlistResult) bool {
			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		flags := []git.Option{
			git.Flag{Name: "--in-commit-order"},
			git.Flag{Name: "--objects"},
			git.Flag{Name: "--object-names"},
		}
		if cfg.blobLimit > 0 {
			flags = append(flags, git.Flag{
				Name: fmt.Sprintf("--filter=blob:limit=%d", cfg.blobLimit),
			})
		}
		if cfg.objectType != "" {
			flags = append(flags,
				git.Flag{Name: fmt.Sprintf("--filter=object:type=%s", cfg.objectType)},
				git.Flag{Name: "--filter-provided-objects"},
			)
		}

		revlist, err := repo.Exec(ctx, git.SubCmd{
			Name:  "rev-list",
			Flags: flags,
			Args:  revisions,
		})
		if err != nil {
			sendResult(RevlistResult{Err: err})
			return
		}

		scanner := bufio.NewScanner(revlist)
		for scanner.Scan() {
			// We need to copy the line here because we'll hand it over to the caller
			// asynchronously, and the next call to `Scan()` will overwrite the buffer.
			line := make([]byte, len(scanner.Bytes()))
			copy(line, scanner.Bytes())

			oidAndName := bytes.SplitN(line, []byte{' '}, 2)

			result := RevlistResult{
				OID: git.ObjectID(oidAndName[0]),
			}
			if len(oidAndName) == 2 && len(oidAndName[1]) > 0 {
				result.ObjectName = oidAndName[1]
			}

			if isDone := sendResult(result); isDone {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			sendResult(RevlistResult{
				Err: fmt.Errorf("scanning rev-list output: %w", err),
			})
			return
		}

		if err := revlist.Wait(); err != nil {
			sendResult(RevlistResult{
				Err: fmt.Errorf("rev-list pipeline command: %w", err),
			})
			return
		}
	}()

	return resultChan
}

// RevlistFilter filters the revlistResults from the provided channel with the filter function: if
// the filter returns `false` for a given item, then it will be dropped from the pipeline. Errors
// cannot be filtered and will always be passed through.
func RevlistFilter(ctx context.Context, c <-chan RevlistResult, filter func(RevlistResult) bool) <-chan RevlistResult {
	resultChan := make(chan RevlistResult)
	go func() {
		defer close(resultChan)

		for result := range c {
			if result.Err != nil || filter(result) {
				select {
				case resultChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return resultChan
}

// CatfileInfoResult is a result for the CatfileInfo pipeline step.
type CatfileInfoResult struct {
	// Err is an error which occurred during execution of the pipeline.
	Err error

	// ObjectName is the object name as received from the revlistResultChan.
	ObjectName []byte
	// ObjectInfo is the object info of the object.
	ObjectInfo *catfile.ObjectInfo
}

// CatfileInfo processes revlistResults from the given channel and extracts object information via
// `git cat-file --batch-check`. The returned channel will contain all processed catfile info
// results. Any error received via the channel or encountered in this step will cause the pipeline
// to fail. Context cancellation will gracefully halt the pipeline.
func CatfileInfo(ctx context.Context, catfile catfile.Batch, revlistResultChan <-chan RevlistResult) <-chan CatfileInfoResult {
	resultChan := make(chan CatfileInfoResult)

	go func() {
		defer close(resultChan)

		sendResult := func(result CatfileInfoResult) bool {
			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		for revlistResult := range revlistResultChan {
			if revlistResult.Err != nil {
				sendResult(CatfileInfoResult{Err: revlistResult.Err})
				return
			}

			objectInfo, err := catfile.Info(ctx, revlistResult.OID.Revision())
			if err != nil {
				sendResult(CatfileInfoResult{
					Err: fmt.Errorf("retrieving object info for %q: %w", revlistResult.OID, err),
				})
				return
			}

			if isDone := sendResult(CatfileInfoResult{
				ObjectName: revlistResult.ObjectName,
				ObjectInfo: objectInfo,
			}); isDone {
				return
			}
		}
	}()

	return resultChan
}

// CatfileInfoAllObjects enumerates all Git objects part of the repository's object directory and
// extracts their object info via `git cat-file --batch-check`. The returned channel will contain
// all processed results. Any error encountered during execution of this pipeline step will cause
// the pipeline to fail. Context cancellation will gracefully halt the pipeline. Note that with this
// pipeline step, the resulting catfileInfoResults will never have an object name.
func CatfileInfoAllObjects(ctx context.Context, repo *localrepo.Repo) <-chan CatfileInfoResult {
	resultChan := make(chan CatfileInfoResult)

	go func() {
		defer close(resultChan)

		sendResult := func(result CatfileInfoResult) bool {
			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		cmd, err := repo.Exec(ctx, git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch-all-objects"},
				git.Flag{Name: "--batch-check"},
				git.Flag{Name: "--buffer"},
				git.Flag{Name: "--unordered"},
			},
		})
		if err != nil {
			sendResult(CatfileInfoResult{
				Err: fmt.Errorf("spawning cat-file failed: %w", err),
			})
			return
		}

		reader := bufio.NewReader(cmd)
		for {
			objectInfo, err := catfile.ParseObjectInfo(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				sendResult(CatfileInfoResult{
					Err: fmt.Errorf("parsing object info: %w", err),
				})
				return
			}

			if isDone := sendResult(CatfileInfoResult{
				ObjectInfo: objectInfo,
			}); isDone {
				return
			}
		}

		if err := cmd.Wait(); err != nil {
			sendResult(CatfileInfoResult{
				Err: fmt.Errorf("cat-file failed: %w", err),
			})
			return
		}
	}()

	return resultChan
}

// CatfileInfoFilter filters the catfileInfoResults from the provided channel with the filter
// function: if the filter returns `false` for a given item, then it will be dropped from the
// pipeline. Errors cannot be filtered and will always be passed through.
func CatfileInfoFilter(ctx context.Context, c <-chan CatfileInfoResult, filter func(CatfileInfoResult) bool) <-chan CatfileInfoResult {
	resultChan := make(chan CatfileInfoResult)
	go func() {
		defer close(resultChan)

		for result := range c {
			if result.Err != nil || filter(result) {
				select {
				case resultChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return resultChan
}

// CatfileObjectResult is a result for the CatfileObject pipeline step.
type CatfileObjectResult struct {
	// Err is an error which occurred during execution of the pipeline.
	Err error

	// ObjectName is the object name as received from the revlistResultChan.
	ObjectName []byte
	// ObjectInfo is the object info of the object.
	ObjectInfo *catfile.ObjectInfo
	// obbjectReader is the reader for the raw object data. The reader must always be consumed
	// by the caller.
	ObjectReader io.Reader
}

type signallingReader struct {
	reader    io.Reader
	doneCh    chan interface{}
	closeOnce sync.Once
}

func (r *signallingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if errors.Is(err, io.EOF) {
		r.closeOnce.Do(func() {
			close(r.doneCh)
		})
	}
	return n, err
}

// CatfileObject processes catfileInfoResults from the given channel and reads associated objects
// into memory via `git cat-file --batch`. The returned channel will contain all processed objects.
// Any error received via the channel or encountered in this step will cause the pipeline to fail.
// Context cancellation will gracefully halt the pipeline. The returned object readers must always
// be fully consumed by the caller.
func CatfileObject(
	ctx context.Context,
	catfileProcess catfile.Batch,
	catfileInfoResultChan <-chan CatfileInfoResult,
) <-chan CatfileObjectResult {
	resultChan := make(chan CatfileObjectResult)
	go func() {
		defer close(resultChan)

		sendResult := func(result CatfileObjectResult) bool {
			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		var objectReader *signallingReader

		for catfileInfoResult := range catfileInfoResultChan {
			if catfileInfoResult.Err != nil {
				sendResult(CatfileObjectResult{Err: catfileInfoResult.Err})
				return
			}

			// We mustn't try to read another object before reading the previous object
			// has concluded. Given that this is not under our control but under the
			// control of the caller, we thus have to wait until the blocking reader has
			// reached EOF.
			if objectReader != nil {
				select {
				case <-objectReader.doneCh:
				case <-ctx.Done():
					return
				}
			}

			var object *catfile.Object
			var err error

			objectType := catfileInfoResult.ObjectInfo.Type
			switch objectType {
			case "tag":
				object, err = catfileProcess.Tag(ctx, catfileInfoResult.ObjectInfo.Oid.Revision())
			case "commit":
				object, err = catfileProcess.Commit(ctx, catfileInfoResult.ObjectInfo.Oid.Revision())
			case "tree":
				object, err = catfileProcess.Tree(ctx, catfileInfoResult.ObjectInfo.Oid.Revision())
			case "blob":
				object, err = catfileProcess.Blob(ctx, catfileInfoResult.ObjectInfo.Oid.Revision())
			default:
				err = fmt.Errorf("unknown object type %q", objectType)
			}

			if err != nil {
				sendResult(CatfileObjectResult{
					Err: fmt.Errorf("requesting object: %w", err),
				})
				return
			}

			objectReader = &signallingReader{
				reader: object,
				doneCh: make(chan interface{}),
			}

			if isDone := sendResult(CatfileObjectResult{
				ObjectName:   catfileInfoResult.ObjectName,
				ObjectInfo:   catfileInfoResult.ObjectInfo,
				ObjectReader: objectReader,
			}); isDone {
				return
			}
		}
	}()

	return resultChan
}
