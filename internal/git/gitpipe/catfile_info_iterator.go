package gitpipe

import "gitlab.com/gitlab-org/gitaly/v14/internal/git"

// CatfileInfoIterator is an iterator returned by the Revlist function.
type CatfileInfoIterator interface {
	ObjectIterator
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

func (it *catfileInfoIterator) ObjectID() git.ObjectID {
	return it.result.ObjectInfo.Oid
}

func (it *catfileInfoIterator) ObjectName() []byte {
	return it.result.ObjectName
}
