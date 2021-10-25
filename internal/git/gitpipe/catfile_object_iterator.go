package gitpipe

import "gitlab.com/gitlab-org/gitaly/v14/internal/git"

// CatfileObjectIterator is an iterator returned by the Revlist function.
type CatfileObjectIterator interface {
	ObjectIterator
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

func (it *catfileObjectIterator) ObjectID() git.ObjectID {
	return it.result.ObjectID()
}

func (it *catfileObjectIterator) ObjectName() []byte {
	return it.result.ObjectName
}
