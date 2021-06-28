package gitpipe

// RevlistIterator is an iterator returned by the Revlist function.
type RevlistIterator interface {
	// Next iterates to the next item. Returns `false` in case there are no more results left.
	Next() bool
	// Err returns the first error that was encountered.
	Err() error
	// Result returns the current item.
	Result() RevlistResult
}

// NewRevlistIterator returns a new RevlistIterator for the given items.
func NewRevlistIterator(items []RevlistResult) RevlistIterator {
	itemChan := make(chan RevlistResult, len(items))
	for _, item := range items {
		itemChan <- item
	}
	close(itemChan)

	return &revlistIterator{
		ch: itemChan,
	}
}

type revlistIterator struct {
	ch     <-chan RevlistResult
	result RevlistResult
}

func (it *revlistIterator) Next() bool {
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

func (it *revlistIterator) Err() error {
	return it.result.err
}

func (it *revlistIterator) Result() RevlistResult {
	return it.result
}
