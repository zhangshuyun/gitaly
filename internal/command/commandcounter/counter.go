package commandcounter

import "sync"

// inFlightCommands tracks the number of in-flight commands. This is hosted outside of the command
// package to avoid a cyclic dependency with the testhelper package.
var inFlightCommands = &sync.WaitGroup{}

// Increment will add another process to the command counter. This may only be called by the
// command package.
func Increment() {
	inFlightCommands.Add(1)
}

// Decrement removes one process from the command counter. This may only be called by the command
// package.
func Decrement() {
	inFlightCommands.Done()
}

// WaitAllDone waits for all commands started by the command package to finish. This can only be
// called once in the lifecycle of the current Go process.
func WaitAllDone() {
	inFlightCommands.Wait()
}
