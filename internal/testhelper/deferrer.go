package testhelper

// Deferrer allows to collect a list of anonymous functions and call them all in reverse order.
// This is useful if you want to make sure that the Deferrer is getting executed in case the
// surrounding function errors, but return the not-yet-executed Deferrer in case it didn't
// by calling Relocate() before the function returns successfully.
type Deferrer []func()

// Add adds another function to the list.
func (c *Deferrer) Add(f func()) {
	*c = append(*c, f)
}

// Call calls all the functions previously added to the list in reverse order.
func (c *Deferrer) Call() {
	for i := len(*c) - 1; i >= 0; i-- {
		(*c)[i]()
	}
}

// Relocate moves all functions into another instance and returns that instance back to the caller.
func (c *Deferrer) Relocate() Deferrer {
	clone := c.clone()
	c.reset()
	return clone
}

// reset removes all functions from the list.
func (c *Deferrer) reset() {
	*c = nil
}

// clone creates a copy of the Deferrer.
func (c *Deferrer) clone() Deferrer {
	clone := make([]func(), len(*c))
	copy(clone, *c)
	return clone
}
