package diff

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

func TestDiffParserWithLargeDiffWithTrueCollapseDiffsFlag(t *testing.T) {
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
		MaxPatchBytes: 100000,
		CollapseDiffs: true,
	}
	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "4cc7061661b8f52891bc1b39feb4d856b21a1067",
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: true,
			lineCount: 100000,
		},
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "3be11c69355948412925fa5e073d76d58ff3afd2",
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 1,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithWordDiff(t *testing.T) {
	rawDiff := `:000000 100644 0000000000000000000000000000000000000000 4cc7061661b8f52891bc1b39feb4d856b21a1067 A	big.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index 000000000..3a62d28e3
--- /dev/null
+++ b/big.txt
@@ -0,0 +1,3 @@
+A
~
+B
~ignoreme
+C
~
`

	limits := Limits{
		EnforceLimits: true,
		SafeMaxFiles:  3,
		SafeMaxBytes:  200,
		SafeMaxLines:  200,
		MaxFiles:      5,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: false,
	}
	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    "0000000000000000000000000000000000000000",
			ToID:      "4cc7061661b8f52891bc1b39feb4d856b21a1067",
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1,3 @@\n+A\n~\n+B\n+C\n~\n"),
			lineCount: 5,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithLargeDiffWithFalseCollapseDiffsFlag(t *testing.T) {
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
		MaxFiles:      4,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: false,
	}

	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "4cc7061661b8f52891bc1b39feb4d856b21a1067",
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: false,
			lineCount: 100000,
			TooLarge:  true,
		},
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "3be11c69355948412925fa5e073d76d58ff3afd2",
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 1,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithLargeDiffWithFalseCollapseDiffsAndCustomPatchLimitFlag(t *testing.T) {
	bigPatch := "@@ -0,0 +1,100000 @@\n" + strings.Repeat("+Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua\n", 1000)
	rawDiff := fmt.Sprintf(`:000000 100644 0000000000000000000000000000000000000000 4cc7061661b8f52891bc1b39feb4d856b21a1067 A	big.txt
:000000 100644 0000000000000000000000000000000000000000 3be11c69355948412925fa5e073d76d58ff3afd2 A	file-00.txt

diff --git a/big.txt b/big.txt
new file mode 100644
index 0000000000000000000000000000000000000000..4cc7061661b8f52891bc1b39feb4d856b21a1067
--- /dev/null
+++ b/big.txt
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
		MaxFiles:      4,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 125000, // bumping from default 100KB to 125kb (first patch has 124.6KB)
		CollapseDiffs: false,
	}

	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "4cc7061661b8f52891bc1b39feb4d856b21a1067",
			FromPath:  []byte("big.txt"),
			ToPath:    []byte("big.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(bigPatch),
			lineCount: 1000,
			TooLarge:  false,
		},
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "3be11c69355948412925fa5e073d76d58ff3afd2",
			FromPath:  []byte("file-00.txt"),
			ToPath:    []byte("file-00.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte("@@ -0,0 +1 @@\n+Lorem ipsum\n"),
			lineCount: 1,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffParserWithLargeDiffOfSmallPatches(t *testing.T) {
	patch := "@@ -0,0 +1,5 @@\n" + strings.Repeat("+Lorem\n", 5)
	rawDiff := `:000000 100644 0000000000000000000000000000000000000000 b6507e5b5ce18077e3ec8aaa2291404e5051d45d A	expand-collapse/file-0.txt
:000000 100644 0000000000000000000000000000000000000000 b6507e5b5ce18077e3ec8aaa2291404e5051d45d A	expand-collapse/file-1.txt
:000000 100644 0000000000000000000000000000000000000000 b6507e5b5ce18077e3ec8aaa2291404e5051d45d A	expand-collapse/file-2.txt

`

	// Create 3 files of 5 lines. The first two files added together surpass
	// the limits, which should cause the last one to be collapsed.
	for i := 0; i < 3; i++ {
		rawDiff += fmt.Sprintf(`diff --git a/expand-collapse/file-%d.txt b/expand-collapse/file-%d.txt
new file mode 100644
index 0000000000000000000000000000000000000000..b6507e5b5ce18077e3ec8aaa2291404e5051d45d
--- /dev/null
+++ b/expand-collapse/file-%d.txt
%s`, i, i, i, patch)
	}

	limits := Limits{
		EnforceLimits: true,
		SafeMaxLines:  10, // This is the one we care about here
		SafeMaxFiles:  10000000,
		SafeMaxBytes:  10000000,
		MaxFiles:      10000000,
		MaxBytes:      10000000,
		MaxLines:      10000000,
		MaxPatchBytes: 100000,
		CollapseDiffs: true,
	}
	diffs := getDiffs(t, rawDiff, limits)

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "b6507e5b5ce18077e3ec8aaa2291404e5051d45d",
			FromPath:  []byte("expand-collapse/file-0.txt"),
			ToPath:    []byte("expand-collapse/file-0.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(patch),
			lineCount: 5,
		},
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "b6507e5b5ce18077e3ec8aaa2291404e5051d45d",
			FromPath:  []byte("expand-collapse/file-1.txt"),
			ToPath:    []byte("expand-collapse/file-1.txt"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(patch),
			lineCount: 5,
		},
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "b6507e5b5ce18077e3ec8aaa2291404e5051d45d",
			FromPath:  []byte("expand-collapse/file-2.txt"),
			ToPath:    []byte("expand-collapse/file-2.txt"),
			Status:    'A',
			Collapsed: true,
			Patch:     nil,
			lineCount: 5,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffLongLine(t *testing.T) {
	header := `:000000 100644 0000000000000000000000000000000000000000 c3ae147b03a2d1fd89b25198b3fc53028c5b0d53 A	file-0

diff --git a/file-0 b/file-0
new file mode 100644
index 0000000000000000000000000000000000000000..c3ae147b03a2d1fd89b25198b3fc53028c5b0d53
--- /dev/null
+++ b/file-0
`
	patch := "@@ -0,0 +1,100 @@\n+" + strings.Repeat("z", 100*1000)

	limits := Limits{
		MaxPatchBytes: 1000 * 1000,
	}
	diffs := getDiffs(t, header+patch, limits)

	expectedDiffs := []*Diff{
		&Diff{
			OldMode:   0,
			NewMode:   0100644,
			FromID:    git.ZeroOID.String(),
			ToID:      "c3ae147b03a2d1fd89b25198b3fc53028c5b0d53",
			FromPath:  []byte("file-0"),
			ToPath:    []byte("file-0"),
			Status:    'A',
			Collapsed: false,
			Patch:     []byte(patch),
			lineCount: 1,
		},
	}

	require.Equal(t, expectedDiffs, diffs)
}

func TestDiffLimitsBeingEnforcedByUpperBound(t *testing.T) {
	limits := Limits{
		SafeMaxLines:  999999999,
		SafeMaxFiles:  999999999,
		SafeMaxBytes:  999999999,
		MaxFiles:      999999999,
		MaxBytes:      0,
		MaxLines:      0,
		MaxPatchBytes: 0,
	}
	diffParser := NewDiffParser(strings.NewReader(""), limits)

	require.Equal(t, diffParser.limits.SafeMaxBytes, safeMaxBytesUpperBound)
	require.Equal(t, diffParser.limits.SafeMaxFiles, safeMaxFilesUpperBound)
	require.Equal(t, diffParser.limits.SafeMaxLines, safeMaxLinesUpperBound)
	require.Equal(t, diffParser.limits.MaxFiles, maxFilesUpperBound)
	require.Equal(t, diffParser.limits.MaxBytes, 0)
	require.Equal(t, diffParser.limits.MaxLines, 0)
	require.Equal(t, diffParser.limits.MaxPatchBytes, 0)
}

func getDiffs(t testing.TB, rawDiff string, limits Limits) []*Diff {
	t.Helper()

	diffParser := NewDiffParser(strings.NewReader(rawDiff), limits)

	diffs := []*Diff{}
	for diffParser.Parse() {
		// Make a deep copy of diffParser.Diff()
		d := *diffParser.Diff()
		for _, p := range []*[]byte{&d.FromPath, &d.ToPath, &d.Patch} {
			*p = append([]byte(nil), *p...)
		}

		diffs = append(diffs, &d)
	}
	require.NoError(t, diffParser.Err())

	return diffs
}

// BenchmarkParserMemory is meant to benchmark memory allocations in the
// parser. Run with 'go test -bench=. -benchmem'.
func BenchmarkParserMemory(b *testing.B) {
	const NDiffs = 10000

	diffData := &bytes.Buffer{}
	for i := 0; i < NDiffs; i++ {
		fmt.Fprintf(diffData, ":000000 100644 0000000000000000000000000000000000000000 c3ae147b03a2d1fd89b25198b3fc53028c5b0d53 A	file-%d\n", i)
	}
	fmt.Fprintln(diffData)
	for i := 0; i < NDiffs; i++ {
		fmt.Fprintf(diffData, `diff --git a/file-%d b/file-%d
new file mode 100644
index 0000000000000000000000000000000000000000..c3ae147b03a2d1fd89b25198b3fc53028c5b0d53
--- /dev/null
+++ b/file-%d
@@ -0,0 +1,100 @@
`, i, i, i)
		for j := 0; j < 100; j++ {
			fmt.Fprintln(diffData, "+zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
		}
	}

	b.Run("parse", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			parser := NewDiffParser(bytes.NewReader(diffData.Bytes()), Limits{})
			n := 0
			for parser.Parse() {
				n++
			}
			require.NoError(b, parser.Err())
			require.Equal(b, NDiffs, n)
		}
	})
}
