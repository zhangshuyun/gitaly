package conflict

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFile_Resolve(t *testing.T) {
	for _, tt := range []struct {
		name         string
		path         string
		conflictFile io.Reader
		parseErr     error
		resolution   Resolution
		resolveErr   error
		expect       string
	}{
		{
			name: "ours",
			path: "conflict.txt",
			conflictFile: strings.NewReader(`# this file is very conflicted
<<<<<<< conflict.txt
we want this line
=======
but they want this line
>>>>>>> conflict.txt
we can both agree on this line though
`),
			resolution: Resolution{
				NewPath: "conflict.txt",
				OldPath: "conflict.txt",
				Sections: map[string]string{
					"dc1c302824bab8da29f7c06fec1c77cf16b975e6_2_2": "head",
				},
			},
			expect: `# this file is very conflicted
we want this line
we can both agree on this line though
`,
		},
		{
			name: "theirs",
			path: "conflict.txt",
			conflictFile: strings.NewReader(`# this file is very conflicted
<<<<<<< conflict.txt
we want this line
=======
but they want this line
>>>>>>> conflict.txt
we can both agree on this line though
`),
			resolution: Resolution{
				NewPath: "conflict.txt",
				OldPath: "conflict.txt",
				Sections: map[string]string{
					"dc1c302824bab8da29f7c06fec1c77cf16b975e6_2_2": "origin",
				},
			},
			expect: `# this file is very conflicted
but they want this line
we can both agree on this line though
`,
		},
		{
			name: "UnexpectedDelimiter",
			path: "conflict.txt",
			conflictFile: strings.NewReader(`# this file is very conflicted
<<<<<<< conflict.txt
we want this line
<<<<<<< conflict.txt
=======
but they want this line
>>>>>>> conflict.txt
we can both agree on this line though
`),
			parseErr: ErrUnexpectedDelimiter,
		},
		{
			name: "ErrMissingEndDelimiter",
			path: "conflict.txt",
			conflictFile: strings.NewReader(`# this file is very conflicted
<<<<<<< conflict.txt
we want this line
=======
but they want this line
we can both agree on this line though
`),
			parseErr: ErrMissingEndDelimiter,
		},
		{
			name:         "Conflict file under file limit",
			path:         "conflict.txt",
			conflictFile: strings.NewReader(strings.Repeat("x", fileLimit-2) + "\n"),
		},
		{
			name:         "ErrUnmergeableFile over file limit",
			path:         "conflict.txt",
			conflictFile: strings.NewReader(strings.Repeat("x", fileLimit+1)),
			parseErr:     ErrUnmergeableFile,
		},
		{
			name:         "ErrUnmergeableFile empty file",
			path:         "conflict.txt",
			conflictFile: strings.NewReader(""),
			parseErr:     ErrUnmergeableFile,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			entry := Entry{
				Path:     tt.path,
				Mode:     0644,
				Contents: []byte("something-with-trailing-newline\n"),
			}

			f, err := Parse(tt.conflictFile, &entry, &entry, &entry)
			require.Equal(t, tt.parseErr, err)

			actual, err := f.Resolve(tt.resolution)
			require.Equal(t, tt.resolveErr, err)
			require.Equal(t, tt.expect, string(actual))
		})
	}
}
