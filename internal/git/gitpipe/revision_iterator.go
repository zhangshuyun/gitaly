package gitpipe

import "gitlab.com/gitlab-org/gitaly/v14/internal/git"

// RevisionIterator is an iterator returned by the Revlist function.
type RevisionIterator interface {
	ObjectIterator
	// Result returns the current item.
	Result() RevisionResult
}

// NewRevisionIterator returns a new RevisionIterator for the given items.
func NewRevisionIterator(items []RevisionResult) RevisionIterator {
	itemChan := make(chan RevisionResult, len(items))
	for _, item := range items {
		itemChan <- item
	}
	close(itemChan)

	return &revisionIterator{
		ch: itemChan,
	}
}

type revisionIterator struct {
	ch     <-chan RevisionResult
	result RevisionResult
}

func (it *revisionIterator) Next() bool {
	if it.result.err != nil {
		return false
	}

	var ok bool
	it.result, ok = <-it.ch
	if !ok || it.result.err != nil {
		return false
	}

	return true
}

func (it *revisionIterator) Err() error {
	return it.result.err
}

func (it *revisionIterator) Result() RevisionResult {
	return it.result
}

func (it *revisionIterator) ObjectID() git.ObjectID {
	return it.result.OID
}

func (it *revisionIterator) ObjectName() []byte {
	return it.result.ObjectName
}
