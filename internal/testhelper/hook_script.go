package testhelper

import "fmt"

const scriptWrapper = `
#!/bin/sh\
%s
exit %d`

// SuccessfulHookScript creates a bash script that will exit with a success code
func SuccessfulHookScript(scriptContent string) string {
	return script(scriptContent, 0)
}

// FailingHookScript creates a bash script that will exit with a failure code
func FailingHookScript(scriptContent string) string {
	return script(scriptContent, 1)
}

func script(scriptContent string, exitCode int8) string {
	return fmt.Sprintf(scriptWrapper, scriptContent, exitCode)
}
