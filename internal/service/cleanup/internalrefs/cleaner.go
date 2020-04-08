package internalrefs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Only references in these namespaces are cleaned up
var internalRefs = []string{
	"refs/environments/",
	"refs/keep-around/",
	"refs/merge-requests/",
}

// A ForEachFunc can be called for every entry in the BFG object map file that
// the cleaner is processing. Returning an error will stop the  cleaner before
// it has processed the entry in question
type ForEachFunc func(oldOID, newOID string, isInternalRef bool) error

// Cleaner is responsible for updating the internal references in a repository
// as specified by a BFG object map. Currently, internal references pointing to
// a commit that has been rewritten will simply be removed.
type Cleaner struct {
	ctx     context.Context
	forEach ForEachFunc

	// Map of SHA -> reference names
	table   map[string][]string
	updater *updateref.Updater
}

// ErrInvalidObjectMap is returned with descriptive text if the supplied object
// map file is in the wrong format
type ErrInvalidObjectMap error

// NewCleaner builds a new instance of Cleaner, which is used to apply a BFG
// object map to a repository.
func NewCleaner(ctx context.Context, repo *gitalypb.Repository, forEach ForEachFunc) (*Cleaner, error) {
	table, err := buildLookupTable(ctx, repo)
	if err != nil {
		return nil, err
	}

	updater, err := updateref.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	return &Cleaner{ctx: ctx, table: table, updater: updater, forEach: forEach}, nil
}

// ApplyObjectMap processes a BFG object map file, removing any internal
// references that point to a rewritten commit.
func (c *Cleaner) ApplyObjectMap(reader io.Reader) error {
	scanner := bufio.NewScanner(reader)
	for i := int64(0); scanner.Scan(); i++ {
		line := scanner.Text()

		// Each line consists of two SHAs: the SHA of the original object, and
		// the SHA of a replacement object in the new history built by BFG. For
		// now, the new SHA is ignored, but it may be used to rewrite (rather
		// than remove) some references in the future.
		shas := strings.SplitN(line, " ", 2)

		if len(shas) != 2 || len(shas[0]) != 40 || len(shas[1]) != 40 {
			return ErrInvalidObjectMap(fmt.Errorf("object map invalid at line %d", i))
		}

		if err := c.processEntry(shas[0], shas[1]); err != nil {
			return err
		}
	}

	return c.updater.Wait()
}

func (c *Cleaner) processEntry(oldSHA, newSHA string) error {
	refs, isPresent := c.table[oldSHA]

	if c.forEach != nil {
		if err := c.forEach(oldSHA, newSHA, isPresent); err != nil {
			return err
		}
	}

	if !isPresent {
		return nil
	}

	ctxlogrus.Extract(c.ctx).WithFields(log.Fields{
		"sha":  oldSHA,
		"refs": refs,
	}).Info("removing internal references")

	// Remove the internal refs pointing to oldSHA
	for _, ref := range refs {
		if err := c.updater.Delete(ref); err != nil {
			return err
		}
	}

	return nil
}

// buildLookupTable constructs an in-memory map of SHA -> refs. Multiple refs
// may point to the same SHA.
//
// The lookup table is necessary to efficiently check which references point to
// an object that has been rewritten by the BFG (and so require action). It is
// consulted once per line in the object map. Git is optimized for ref -> SHA
// lookups, but we want the opposite!
func buildLookupTable(ctx context.Context, repo *gitalypb.Repository) (map[string][]string, error) {
	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
		Name:  "for-each-ref",
		Flags: []git.Option{git.ValueFlag{Name: "--format", Value: "%(objectname) %(refname)"}},
		Args:  internalRefs,
	})
	if err != nil {
		return nil, err
	}

	logger := ctxlogrus.Extract(ctx)
	out := make(map[string][]string)
	scanner := bufio.NewScanner(cmd)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 || len(parts[0]) != 40 {
			logger.WithFields(log.Fields{"line": line}).Warn("failed to parse git refs")
			return nil, fmt.Errorf("failed to parse git refs")
		}

		out[parts[0]] = append(out[parts[0]], parts[1])
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
