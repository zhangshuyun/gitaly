package supervisor

// EventType is used to label Event instances.
type EventType int

const (
	// Up is a notification that the process with the accompanying PID is up.
	Up EventType = iota
	// Crash is a notification that the process has either failed to spawn or crashed.
	// In case creating the process failed, the PID will be set to `-1`.
	Crash
	// MemoryHigh is a notification that process memory for the current PID
	// exceeds the threshold.
	MemoryHigh
	// MemoryLow indicates the process memory is at or below the threshold.
	MemoryLow
	// HealthOK indicates the that the health check function returned a 'nil' error
	HealthOK
	// HealthBad indicates the that the health check function returned an error
	HealthBad
)

// Event is used to notify a listener of process state changes.
type Event struct {
	Type  EventType
	Pid   int
	Error error
}
