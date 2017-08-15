package smarthttp

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestSuccessfulScanDeepen(t *testing.T) {
	examples := []struct {
		input  string
		output bool
	}{
		{"000dsomething000cdeepen 10000", true},
		{"000dsomething0000000cdeepen 1", true},
		{"000dsomething0000000cdeepen 1" + strings.Repeat("garbage", 1000000), true},
		{"000dsomething0000", false},
	}

	for _, example := range examples {
		t.Run(fmt.Sprintf(".30%s", example.input), func(t *testing.T) {
			reader := bytes.NewReader([]byte(example.input))
			hasDeepen := scanDeepen(reader)
			if n := reader.Len(); n != 0 {
				t.Fatalf("expected reader to be drained, found %d bytes left", n)
			}

			if hasDeepen != example.output {
				t.Fatalf("expected %v, got %v", example.output, hasDeepen)
			}
		})
	}
}

func TestFailedScanDeepen(t *testing.T) {
	examples := []string{
		"invalid data",
		"deepen",
		"000cdeepen",
	}

	for _, example := range examples {
		t.Run(example, func(t *testing.T) {
			if scanDeepen(bytes.NewReader([]byte(example))) == true {
				t.Fatalf("scanDeepen %q: expected result to be false, got true", example)
			}
		})
	}
}
