package diff

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiffParserWithLargeDiff(t *testing.T) {
	bigPatch := strings.Repeat("+Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n", 100000)
	rawDiff := fmt.Sprintf(`:000000 100644 0000000000000000000000000000000000000000 4cc7061661b8f52891bc1b39feb4d856b21a1067 A	big.txt
:000000 100644 0000000000000000000000000000000000000000 3be11c69355948412925fa5e073d76d58ff3afd2 A	file-00.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index 0000000000000000000000000000000000000000..4cc7061661b8f52891bc1b39feb4d856b21a1067
--- /dev/null
+++ b/big.txt
@@ -0,0 +1,100000 @@
%sdiff --git a/file-00.txt b/file-00.txt
new file mode 100644
index 0000000000000000000000000000000000000000..3be11c69355948412925fa5e073d76d58ff3afd2
--- /dev/null
+++ b/file-00.txt
@@ -0,0 +1 @@
+Lorem ipsum
`, bigPatch)

	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      5,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		CollapseDiffs: true,
	}
	diffParser := NewDiffParser(strings.NewReader(rawDiff), limits)

	diffs := []*Diff{}
	for diffParser.Parse() {
		diffs = append(diffs, diffParser.Diff())
	}

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    "0000000000000000000000000000000000000000",
			ToID:      "4cc7061661b8f52891bc1b39feb4d856b21a1067",
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    65,
			Collapsed: true,
			lineCount: 100003,
		},
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    "0000000000000000000000000000000000000000",
			ToID:      "3be11c69355948412925fa5e073d76d58ff3afd2",
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    65,
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 4,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}
