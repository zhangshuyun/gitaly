package git

// StaticOption are reusable trusted options
type StaticOption struct {
	value string
}

// OptionArgs just passes through the already trusted value. This never
// returns an error.
func (sa StaticOption) OptionArgs() ([]string, error) { return []string{sa.value}, nil }

// OutputToStdout is used indicate the output should be sent to STDOUT
// Seen in: git bundle create
var OutputToStdout = StaticOption{value: "-"}
