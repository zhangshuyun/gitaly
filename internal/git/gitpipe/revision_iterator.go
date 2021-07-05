package gitpipe

// RevisionIterator is an iterator returned by the Revlist function.
type RevisionIterator interface {
	// Next iterates to the next item. Returns `false` in case there are no more results left.
	Next() bool
	// Err returns the first error that was encountered.
	Err() error
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
