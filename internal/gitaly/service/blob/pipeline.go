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

// revlistResult is a result for the revlist pipeline step.
type revlistResult struct {
	// err is an error which occurred during execution of the pipeline.
	err error

	// oid is the object ID of an object printed by git-rev-list(1).
	oid git.ObjectID
	// objectName is the name of the object. This is typically the path of the object if it was
	// traversed via either a tree or a commit. The path depends on the order in which objects
	// are traversed: if e.g. two different trees refer to the same blob with different names,
	// the blob's path depends on which of the trees was traversed first.
	objectName []byte
}

type objectType string

const (
	objectTypeCommit = objectType("commit")
	objectTypeBlob   = objectType("blob")
	objectTypeTree   = objectType("tree")
	objectTypeTag    = objectType("tag")
)

// revlistConfig is configuration for the revlist pipeline step.
type revlistConfig struct {
	blobLimit  int
	objectType objectType
}

// revlistOption is an option for the revlist pipeline step.
type revlistOption func(cfg *revlistConfig)

// withBlobLimit sets up a size limit for blobs. Only blobs whose size is smaller than this limit
// will be returned by the pipeline step.
func withBlobLimit(limit int) revlistOption {
	return func(cfg *revlistConfig) {
		cfg.blobLimit = limit
	}
}

// withObjectTypeFilter will set up a `--filter=object:type=` filter for git-rev-list(1). This will
// cause it to filter out any objects which do not match the given type. Because git-rev-list(1) by
// default never filters provided arguments, this option also sets up the `--filter-provided` flag.
// Note that this option is only supported starting with Git v2.32.0 or later.
func withObjectTypeFilter(t objectType) revlistOption {
	return func(cfg *revlistConfig) {
		cfg.objectType = t
	}
}

// revlist runs git-rev-list(1) with objects and object names enabled. The returned channel will
// contain all object IDs listed by this command. Cancelling the context will cause the pipeline to
// be cancelled, too.
func revlist(
	ctx context.Context,
	repo *localrepo.Repo,
	revisions []string,
	options ...revlistOption,
) <-chan revlistResult {
	var cfg revlistConfig
	for _, option := range options {
		option(&cfg)
	}

	resultChan := make(chan revlistResult)
	go func() {
		defer close(resultChan)

		sendResult := func(result revlistResult) bool {
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
			sendResult(revlistResult{err: err})
			return
		}

		scanner := bufio.NewScanner(revlist)
		for scanner.Scan() {
			// We need to copy the line here because we'll hand it over to the caller
			// asynchronously, and the next call to `Scan()` will overwrite the buffer.
			line := make([]byte, len(scanner.Bytes()))
			copy(line, scanner.Bytes())

			oidAndName := bytes.SplitN(line, []byte{' '}, 2)

			result := revlistResult{
				oid: git.ObjectID(oidAndName[0]),
			}
			if len(oidAndName) == 2 && len(oidAndName[1]) > 0 {
				result.objectName = oidAndName[1]
			}

			if isDone := sendResult(result); isDone {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			sendResult(revlistResult{
				err: fmt.Errorf("scanning rev-list output: %w", err),
			})
			return
		}

		if err := revlist.Wait(); err != nil {
			sendResult(revlistResult{
				err: fmt.Errorf("rev-list pipeline command: %w", err),
			})
			return
		}
	}()

	return resultChan
}

// revlistFilter filters the revlistResults from the provided channel with the filter function: if
// the filter returns `false` for a given item, then it will be dropped from the pipeline. Errors
// cannot be filtered and will always be passed through.
func revlistFilter(ctx context.Context, c <-chan revlistResult, filter func(revlistResult) bool) <-chan revlistResult {
	resultChan := make(chan revlistResult)
	go func() {
		defer close(resultChan)

		for result := range c {
			if result.err != nil || filter(result) {
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

// catfileInfoResult is a result for the catfileInfo pipeline step.
type catfileInfoResult struct {
	// err is an error which occurred during execution of the pipeline.
	err error

	// objectName is the object name as received from the revlistResultChan.
	objectName []byte
	// objectInfo is the object info of the object.
	objectInfo *catfile.ObjectInfo
}

// catfileInfo processes revlistResults from the given channel and extracts object information via
// `git cat-file --batch-check`. The returned channel will contain all processed catfile info
// results. Any error received via the channel or encountered in this step will cause the pipeline
// to fail. Context cancellation will gracefully halt the pipeline.
func catfileInfo(ctx context.Context, catfile catfile.Batch, revlistResultChan <-chan revlistResult) <-chan catfileInfoResult {
	resultChan := make(chan catfileInfoResult)

	go func() {
		defer close(resultChan)

		sendResult := func(result catfileInfoResult) bool {
			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		for revlistResult := range revlistResultChan {
			if revlistResult.err != nil {
				sendResult(catfileInfoResult{err: revlistResult.err})
				return
			}

			objectInfo, err := catfile.Info(ctx, revlistResult.oid.Revision())
			if err != nil {
				sendResult(catfileInfoResult{
					err: fmt.Errorf("retrieving object info for %q: %w", revlistResult.oid, err),
				})
				return
			}

			if isDone := sendResult(catfileInfoResult{
				objectName: revlistResult.objectName,
				objectInfo: objectInfo,
			}); isDone {
				return
			}
		}
	}()

	return resultChan
}

// catfileInfoAllObjects enumerates all Git objects part of the repository's object directory and
// extracts their object info via `git cat-file --batch-check`. The returned channel will contain
// all processed results. Any error encountered during execution of this pipeline step will cause
// the pipeline to fail. Context cancellation will gracefully halt the pipeline. Note that with this
// pipeline step, the resulting catfileInfoResults will never have an object name.
func catfileInfoAllObjects(ctx context.Context, repo *localrepo.Repo) <-chan catfileInfoResult {
	resultChan := make(chan catfileInfoResult)

	go func() {
		defer close(resultChan)

		sendResult := func(result catfileInfoResult) bool {
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
			sendResult(catfileInfoResult{
				err: fmt.Errorf("spawning cat-file failed: %w", err),
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

				sendResult(catfileInfoResult{
					err: fmt.Errorf("parsing object info: %w", err),
				})
				return
			}

			if isDone := sendResult(catfileInfoResult{
				objectInfo: objectInfo,
			}); isDone {
				return
			}
		}

		if err := cmd.Wait(); err != nil {
			sendResult(catfileInfoResult{
				err: fmt.Errorf("cat-file failed: %w", err),
			})
			return
		}
	}()

	return resultChan
}

// catfileInfoFilter filters the catfileInfoResults from the provided channel with the filter
// function: if the filter returns `false` for a given item, then it will be dropped from the
// pipeline. Errors cannot be filtered and will always be passed through.
func catfileInfoFilter(ctx context.Context, c <-chan catfileInfoResult, filter func(catfileInfoResult) bool) <-chan catfileInfoResult {
	resultChan := make(chan catfileInfoResult)
	go func() {
		defer close(resultChan)

		for result := range c {
			if result.err != nil || filter(result) {
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

// catfileObjectResult is a result for the catfileObject pipeline step.
type catfileObjectResult struct {
	// err is an error which occurred during execution of the pipeline.
	err error

	// objectName is the object name as received from the revlistResultChan.
	objectName []byte
	// objectInfo is the object info of the object.
	objectInfo *catfile.ObjectInfo
	// obbjectReader is the reader for the raw object data. The reader must always be consumed
	// by the caller.
	objectReader io.Reader
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

// catfileObject processes catfileInfoResults from the given channel and reads associated objects
// into memory via `git cat-file --batch`. The returned channel will contain all processed objects.
// Any error received via the channel or encountered in this step will cause the pipeline to fail.
// Context cancellation will gracefully halt the pipeline. The returned object readers must always
// be fully consumed by the caller.
func catfileObject(
	ctx context.Context,
	catfileProcess catfile.Batch,
	catfileInfoResultChan <-chan catfileInfoResult,
) <-chan catfileObjectResult {
	resultChan := make(chan catfileObjectResult)
	go func() {
		defer close(resultChan)

		sendResult := func(result catfileObjectResult) bool {
			select {
			case resultChan <- result:
				return false
			case <-ctx.Done():
				return true
			}
		}

		var objectReader *signallingReader

		for catfileInfoResult := range catfileInfoResultChan {
			if catfileInfoResult.err != nil {
				sendResult(catfileObjectResult{err: catfileInfoResult.err})
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

			objectType := catfileInfoResult.objectInfo.Type
			switch objectType {
			case "tag":
				object, err = catfileProcess.Tag(ctx, catfileInfoResult.objectInfo.Oid.Revision())
			case "commit":
				object, err = catfileProcess.Commit(ctx, catfileInfoResult.objectInfo.Oid.Revision())
			case "tree":
				object, err = catfileProcess.Tree(ctx, catfileInfoResult.objectInfo.Oid.Revision())
			case "blob":
				object, err = catfileProcess.Blob(ctx, catfileInfoResult.objectInfo.Oid.Revision())
			default:
				err = fmt.Errorf("unknown object type %q", objectType)
			}

			if err != nil {
				sendResult(catfileObjectResult{
					err: fmt.Errorf("requesting object: %w", err),
				})
				return
			}

			objectReader = &signallingReader{
				reader: object,
				doneCh: make(chan interface{}),
			}

			if isDone := sendResult(catfileObjectResult{
				objectName:   catfileInfoResult.objectName,
				objectInfo:   catfileInfoResult.objectInfo,
				objectReader: objectReader,
			}); isDone {
				return
			}
		}
	}()

	return resultChan
}
