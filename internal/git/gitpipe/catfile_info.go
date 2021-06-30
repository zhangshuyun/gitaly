package gitpipe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
)

// CatfileInfoResult is a result for the CatfileInfo pipeline step.
type CatfileInfoResult struct {
	// err is an error which occurred during execution of the pipeline.
	err error

	// ObjectName is the object name as received from the revlistResultChan.
	ObjectName []byte
	// ObjectInfo is the object info of the object.
	ObjectInfo *catfile.ObjectInfo
}

// CatfileInfo processes revlistResults from the given channel and extracts object information via
// `git cat-file --batch-check`. The returned channel will contain all processed catfile info
// results. Any error received via the channel or encountered in this step will cause the pipeline
// to fail. Context cancellation will gracefully halt the pipeline.
func CatfileInfo(ctx context.Context, catfile catfile.Batch, revlistIterator RevlistIterator) CatfileInfoIterator {
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

		for revlistIterator.Next() {
			revlistResult := revlistIterator.Result()

			objectInfo, err := catfile.Info(ctx, revlistResult.OID.Revision())
			if err != nil {
				sendResult(CatfileInfoResult{
					err: fmt.Errorf("retrieving object info for %q: %w", revlistResult.OID, err),
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

		if err := revlistIterator.Err(); err != nil {
			sendResult(CatfileInfoResult{err: err})
			return
		}
	}()

	return &catfileInfoIterator{
		ch: resultChan,
	}
}

// CatfileInfoAllObjects enumerates all Git objects part of the repository's object directory and
// extracts their object info via `git cat-file --batch-check`. The returned channel will contain
// all processed results. Any error encountered during execution of this pipeline step will cause
// the pipeline to fail. Context cancellation will gracefully halt the pipeline. Note that with this
// pipeline step, the resulting catfileInfoResults will never have an object name.
func CatfileInfoAllObjects(ctx context.Context, repo *localrepo.Repo) CatfileInfoIterator {
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

				sendResult(CatfileInfoResult{
					err: fmt.Errorf("parsing object info: %w", err),
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
				err: fmt.Errorf("cat-file failed: %w", err),
			})
			return
		}
	}()

	return &catfileInfoIterator{
		ch: resultChan,
	}
}

// CatfileInfoFilter filters the catfileInfoResults from the provided channel with the filter
// function: if the filter returns `false` for a given item, then it will be dropped from the
// pipeline. Errors cannot be filtered and will always be passed through.
func CatfileInfoFilter(ctx context.Context, it CatfileInfoIterator, filter func(CatfileInfoResult) bool) CatfileInfoIterator {
	resultChan := make(chan CatfileInfoResult)

	go func() {
		defer close(resultChan)

		for it.Next() {
			result := it.Result()
			if filter(result) {
				select {
				case resultChan <- result:
				case <-ctx.Done():
					return
				}
			}
		}

		if err := it.Err(); err != nil {
			select {
			case resultChan <- CatfileInfoResult{err: err}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return &catfileInfoIterator{
		ch: resultChan,
	}
}
