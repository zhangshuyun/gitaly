package maintenance

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
)

var errIterOver = errors.New("random walker at end")

type stackFrame struct {
	name    string
	entries []os.DirEntry
}

// randomWalker is a filesystem walker which traverses a directory hierarchy in depth-first order,
// randomizing the order of each directory's entries.
type randomWalker struct {
	stack      []stackFrame
	root       string
	pendingDir string
	rand       *rand.Rand
}

// newRandomWalker creates a new random walker starting at `root`.
func newRandomWalker(root string, r *rand.Rand) *randomWalker {
	return &randomWalker{
		pendingDir: root,
		root:       root,
		rand:       r,
	}
}

// next returns the next file. Traversal happens in depth-first order, where each directory's
// entities are traversed in random order. If there are no more files to iterate, `errIterOver` is
// returned.
func (r *randomWalker) next() (os.DirEntry, string, error) {
	if r.pendingDir != "" {
		// Reset pendingDir before returning the error such that the caller can continue if
		// he doesn't care e.g. for the directory not existing.
		pendingDir := r.pendingDir
		r.pendingDir = ""

		entries, err := os.ReadDir(pendingDir)
		if err != nil {
			return nil, pendingDir, err
		}

		shuffleDirEntries(r.rand, entries)
		r.stack = append(r.stack, stackFrame{
			name:    pendingDir,
			entries: entries,
		})
	}

	// Iterate over all stack frames depth-first and search for the first non-empty
	// one. If there are none, then it means that we've finished the depth-first search
	// and return `errIterOver`.
	for {
		if len(r.stack) == 0 {
			return nil, "", errIterOver
		}

		// Retrieve the current bottom-most stack frame. If the stack frame is empty, we pop
		// it and retry its parent frame.
		stackFrame := &r.stack[len(r.stack)-1]
		if len(stackFrame.entries) == 0 {
			r.stack = r.stack[:len(r.stack)-1]
			continue
		}

		fi := stackFrame.entries[0]
		stackFrame.entries = stackFrame.entries[1:]

		path := filepath.Join(stackFrame.name, fi.Name())
		if fi.IsDir() {
			r.pendingDir = path
		}

		return fi, path, nil
	}
}

func shuffleDirEntries(randSrc *rand.Rand, s []os.DirEntry) {
	randSrc.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
}

// skipDir marks the current directory such that it does not get descended into.
func (r *randomWalker) skipDir() {
	r.pendingDir = ""
}
