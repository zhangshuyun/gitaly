package gitpipe

// CatfileInfoIterator is an iterator returned by the Revlist function.
type CatfileInfoIterator interface {
	// Next iterates to the next item. Returns `false` in case there are no more results left.
	Next() bool
	// Err returns the first error that was encountered.
	Err() error
	// Result returns the current item.
	Result() CatfileInfoResult
}

// NewCatfileInfoIterator returns a new CatfileInfoIterator for the given items.
func NewCatfileInfoIterator(items []CatfileInfoResult) CatfileInfoIterator {
	itemChan := make(chan CatfileInfoResult, len(items))
	for _, item := range items {
		itemChan <- item
	}
	close(itemChan)

	return &catfileInfoIterator{
		ch: itemChan,
	}
}

type catfileInfoIterator struct {
	ch     <-chan CatfileInfoResult
	result CatfileInfoResult
}

func (it *catfileInfoIterator) Next() bool {
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

func (it *catfileInfoIterator) Err() error {
	return it.result.err
}

func (it *catfileInfoIterator) Result() CatfileInfoResult {
	return it.result
}
