package gitpipe

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
)

// RevisionResult is a result for the revlist pipeline step.
type RevisionResult struct {
	// err is an error which occurred during execution of the pipeline.
	err error

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
	blobLimit     int
	objects       bool
	objectType    ObjectType
	order         Order
	reverse       bool
	maxParents    uint
	disabledWalk  bool
	firstParent   bool
	before, after time.Time
	author        []byte
}

// RevlistOption is an option for the revlist pipeline step.
type RevlistOption func(cfg *revlistConfig)

// WithObjects will cause git-rev-list(1) to not only list commits, but also objects referenced by
// those commits.
func WithObjects() RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.objects = true
	}
}

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

// Order is the order in which objects are printed.
type Order int

const (
	// OrderNone is the default ordering, which is reverse chronological order.
	OrderNone = Order(iota)
	// OrderTopo will cause no parents to be shown before all of its children are shown.
	// Furthermore, multiple lines of history will not be intermixed.
	OrderTopo
	// OrderDate order will cause no parents to be shown before all of its children are shown.
	// Otherwise, commits are shown in commit timestamp order. This can cause history to be
	// shown intermixed.
	OrderDate
)

// WithOrder will change the ordering of how objects are listed.
func WithOrder(o Order) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.order = o
	}
}

// WithReverse will reverse the ordering of commits.
func WithReverse() RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.reverse = true
	}
}

// WithMaxParents will cause git-rev-list(1) to list only commits with at most p parents. If set to
// 1, then merge commits will be skipped. While the zero-value for git-rev-list(1) would cause it to
// only print the root commit, we use it as the default value and simply print all commits in that
// case.
func WithMaxParents(p uint) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.maxParents = p
	}
}

// WithDisabledWalk will cause git-rev-list(1) to not do a graph walk beyond the immediate specified
// tips.
func WithDisabledWalk() RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.disabledWalk = true
	}
}

// WithFirstParent will cause git-rev-list(1) to only walk down the first-parent chain of commits.
func WithFirstParent() RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.firstParent = true
	}
}

// WithBefore will cause git-rev-list(1) to only show commits older than the specified time.
func WithBefore(t time.Time) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.before = t
	}
}

// WithAfter will cause git-rev-list(1) to only show commits newer than the specified time.
func WithAfter(t time.Time) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.after = t
	}
}

// WithAuthor will cause git-rev-list(1) to only show commits created by an author matching the
// given pattern.
func WithAuthor(author []byte) RevlistOption {
	return func(cfg *revlistConfig) {
		cfg.author = author
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
) RevisionIterator {
	var cfg revlistConfig
	for _, option := range options {
		option(&cfg)
	}

	resultChan := make(chan RevisionResult)
	go func() {
		defer close(resultChan)

		flags := []git.Option{}

		if cfg.objects {
			flags = append(flags,
				git.Flag{Name: "--in-commit-order"},
				git.Flag{Name: "--objects"},
				git.Flag{Name: "--object-names"},
			)
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

		switch cfg.order {
		case OrderNone:
			// Default order, nothing to do.
		case OrderTopo:
			flags = append(flags, git.Flag{Name: "--topo-order"})
		case OrderDate:
			flags = append(flags, git.Flag{Name: "--date-order"})
		}

		if cfg.reverse {
			flags = append(flags, git.Flag{Name: "--reverse"})
		}

		if cfg.maxParents > 0 {
			flags = append(flags, git.Flag{
				Name: fmt.Sprintf("--max-parents=%d", cfg.maxParents)},
			)
		}

		if cfg.disabledWalk {
			flags = append(flags, git.Flag{Name: "--no-walk"})
		}

		if cfg.firstParent {
			flags = append(flags, git.Flag{Name: "--first-parent"})
		}

		if !cfg.before.IsZero() {
			flags = append(flags, git.Flag{
				Name: fmt.Sprintf("--before=%s", cfg.before.String()),
			})
		}

		if !cfg.after.IsZero() {
			flags = append(flags, git.Flag{
				Name: fmt.Sprintf("--after=%s", cfg.after.String()),
			})
		}

		if len(cfg.author) > 0 {
			flags = append(flags, git.Flag{
				Name: fmt.Sprintf("--author=%s", string(cfg.author)),
			})
		}

		revlist, err := repo.Exec(ctx, git.SubCmd{
			Name:  "rev-list",
			Flags: flags,
			Args:  revisions,
		})
		if err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{err: err})
			return
		}

		scanner := bufio.NewScanner(revlist)
		for scanner.Scan() {
			// We need to copy the line here because we'll hand it over to the caller
			// asynchronously, and the next call to `Scan()` will overwrite the buffer.
			line := make([]byte, len(scanner.Bytes()))
			copy(line, scanner.Bytes())

			oidAndName := bytes.SplitN(line, []byte{' '}, 2)

			result := RevisionResult{
				OID: git.ObjectID(oidAndName[0]),
			}
			if len(oidAndName) == 2 && len(oidAndName[1]) > 0 {
				result.ObjectName = oidAndName[1]
			}

			if isDone := sendRevisionResult(ctx, resultChan, result); isDone {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("scanning rev-list output: %w", err),
			})
			return
		}

		if err := revlist.Wait(); err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("rev-list pipeline command: %w", err),
			})
			return
		}
	}()

	return &revisionIterator{
		ch: resultChan,
	}
}

// ForEachRef runs git-for-each-ref(1) with the given patterns and returns a RevisionIterator for
// found references. Patterns must always refer to fully qualified reference names. Patterns for
// which no branch is found do not result in an error. The iterator's object name is set to the
// reference, while its object ID is the target object the reference points to. Cancelling the
// context will cause the pipeline to be cancelled, too.
func ForEachRef(
	ctx context.Context,
	repo *localrepo.Repo,
	patterns []string,
) RevisionIterator {
	resultChan := make(chan RevisionResult)

	go func() {
		defer close(resultChan)

		forEachRef, err := repo.Exec(ctx, git.SubCmd{
			Name: "for-each-ref",
			Flags: []git.Option{
				// The default format also includes the object type, which requires
				// us to read the referenced commit's object. It would thus be about
				// 2-3x slower to use the default format, and instead we move the
				// burden into the next pipeline step.
				git.Flag{Name: "--format=%(objectname) %(refname)"},
			},
			Args: patterns,
		})
		if err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{err: err})
			return
		}

		scanner := bufio.NewScanner(forEachRef)
		for scanner.Scan() {
			line := make([]byte, len(scanner.Bytes()))
			copy(line, scanner.Bytes())

			oidAndRef := bytes.SplitN(line, []byte{' '}, 2)
			if len(oidAndRef) != 2 {
				sendRevisionResult(ctx, resultChan, RevisionResult{
					err: fmt.Errorf("invalid for-each-ref format: %q", line),
				})
				return
			}

			if isDone := sendRevisionResult(ctx, resultChan, RevisionResult{
				OID:        git.ObjectID(oidAndRef[0]),
				ObjectName: oidAndRef[1],
			}); isDone {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("scanning for-each-ref output: %w", err),
			})
			return
		}

		if err := forEachRef.Wait(); err != nil {
			sendRevisionResult(ctx, resultChan, RevisionResult{
				err: fmt.Errorf("for-each-ref pipeline command: %w", err),
			})
			return
		}
	}()

	return &revisionIterator{
		ch: resultChan,
	}
}

// RevisionFilter filters the RevisionResult from the provided iterator with the filter function: if
// the filter returns `false` for a given item, then it will be dropped from the pipeline. Errors
// cannot be filtered and will always be passed through.
func RevisionFilter(ctx context.Context, it RevisionIterator, filter func(RevisionResult) bool) RevisionIterator {
	return RevisionTransform(ctx, it, func(r RevisionResult) []RevisionResult {
		if filter(r) {
			return []RevisionResult{r}
		}
		return []RevisionResult{}
	})
}

// RevisionTransform transforms each RevisionResult from the provided iterator with the transforming
// function. Instead of sending the original RevisionResult, it will instead send transformed
// results.
func RevisionTransform(ctx context.Context, it RevisionIterator, transform func(RevisionResult) []RevisionResult) RevisionIterator {
	resultChan := make(chan RevisionResult)

	go func() {
		defer close(resultChan)

		for it.Next() {
			for _, transformed := range transform(it.Result()) {
				if sendRevisionResult(ctx, resultChan, transformed) {
					return
				}
			}
		}

		if err := it.Err(); err != nil {
			if sendRevisionResult(ctx, resultChan, RevisionResult{err: err}) {
				return
			}
		}
	}()

	return &revisionIterator{
		ch: resultChan,
	}
}

func sendRevisionResult(ctx context.Context, ch chan<- RevisionResult, result RevisionResult) bool {
	// In case the context has been cancelled, we have a race between observing an error from
	// the killed Git process and observing the context cancellation itself. But if we end up
	// here because of cancellation of the Git process, we don't want to pass that one down the
	// pipeline but instead just stop the pipeline gracefully. We thus have this check here up
	// front to error messages from the Git process.
	select {
	case <-ctx.Done():
		return true
	default:
	}

	select {
	case ch <- result:
		return false
	case <-ctx.Done():
		return true
	}
}
