package gitpipe

// CatfileObjectIterator is an iterator returned by the Revlist function.
type CatfileObjectIterator interface {
	// Next iterates to the next item. Returns `false` in case there are no more results left.
	Next() bool
	// Err returns the first error that was encountered.
	Err() error
	// Result returns the current item.
	Result() CatfileObjectResult
}

// NewCatfileObjectIterator returns a new CatfileObjectIterator for the given items.
func NewCatfileObjectIterator(items []CatfileObjectResult) CatfileObjectIterator {
	itemChan := make(chan CatfileObjectResult, len(items))
	for _, item := range items {
		itemChan <- item
	}
	close(itemChan)

	return &catfileObjectIterator{
		ch: itemChan,
	}
}

type catfileObjectIterator struct {
	ch     <-chan CatfileObjectResult
	result CatfileObjectResult
}

func (it *catfileObjectIterator) Next() bool {
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

func (it *catfileObjectIterator) Err() error {
	return it.result.err
}

func (it *catfileObjectIterator) Result() CatfileObjectResult {
	return it.result
}
