package buildhelper

import (
	"os"
	"strings"
	"testing"
)

// SkipFlakyTest will ensure that the test is skipped if the appropriate
// environment variable is enabled. Additionally, the test name will be asserted
// to ensure it follows the appropriate naming scheme.
func SkipFlakyTest(t *testing.T) {
	const testPrefix = "TestFlaky"
	if !strings.HasPrefix(t.Name(), testPrefix) {
		t.Fatalf("test name %q must begin with %q to be marked flaky",
			t.Name(), testPrefix,
		)
	}
	if os.Getenv("ENABLE_FLAKY_TESTS") != "1" {
		t.Skip("flaky test is not enabled")
	}
}
