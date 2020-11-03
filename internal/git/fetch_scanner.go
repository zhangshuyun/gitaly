package git

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type RefUpdateType byte

// type FetchStatusLine represents a line of status output from `git fetch`, as
// documented here: https://git-scm.com/docs/git-fetch/2.11.4#_output
//
// Note that the content of the `from` and `to` lines may be affected by the
// refspecs given to the `git fetch` command
type FetchStatusLine struct {
	Type    RefUpdateType
	Summary string
	From    string
	To      string
	Reason  string
}

const (
	RefUpdateTypeFastForwardUpdate RefUpdateType = ' '
	RefUpdateTypeForcedUpdate      RefUpdateType = '+'
	RefUpdateTypePruned            RefUpdateType = '-'
	RefUpdateTypeTagUpdate         RefUpdateType = 't'
	RefUpdateTypeFetched           RefUpdateType = '*'
	RefUpdateTypeUpdateFailed      RefUpdateType = '!'
	RefUpdateTypeUnchanged         RefUpdateType = '='
)

var (
	validRefUpdateTypes = []RefUpdateType{
		RefUpdateTypeFastForwardUpdate,
		RefUpdateTypeForcedUpdate,
		RefUpdateTypePruned,
		RefUpdateTypeTagUpdate,
		RefUpdateTypeFetched,
		RefUpdateTypeUpdateFailed,
		RefUpdateTypeUnchanged,
	}
)

func (t RefUpdateType) Valid() bool {
	for _, cmp := range validRefUpdateTypes {
		if t == cmp {
			return true
		}
	}

	return false
}

// type FetchScanner scans the output of `git fetch`, allowing information about
// the updated refs to be gathered
type FetchScanner struct {
	scanner  *bufio.Scanner
	lastErr  error
	lastLine FetchStatusLine
}

func NewFetchScanner(r io.Reader) *FetchScanner {
	return &FetchScanner{scanner: bufio.NewScanner(r)}
}

func (f *FetchScanner) Scan() bool {
	if f.lastErr != nil {
		return false
	}

	for f.scanner.Scan() {
		// Silently ignore non-matching lines
		line, ok := parseFetchStatusLine(f.scanner.Bytes())
		if !ok {
			continue
		}

		f.lastLine = line
		return true
	}

	f.lastErr = f.scanner.Err()
	return false
}

func (f *FetchScanner) Err() error {
	return f.lastErr
}

func (f *FetchScanner) StatusLine() FetchStatusLine {
	return f.lastLine
}

// line has this format: " <flag> <summary> <from> -> <to> [<reason>]"
func parseFetchStatusLine(line []byte) (FetchStatusLine, bool) {
	var blank FetchStatusLine
	var out FetchStatusLine

	// Handle the flag very strictly, since status and non-status text mingle
	if len(line) < 4 || line[0] != ' ' || line[2] != ' ' {
		return blank, false
	}

	out.Type, line = RefUpdateType(line[1]), line[3:]
	if !out.Type.Valid() {
		return blank, false
	}

	// Get the summary, which may be composed of multiple words
	if line[0] == '[' {
		end := bytes.IndexByte(line, ']')
		if end < 0 || len(line) <= end+2 {
			return blank, false
		}

		out.Summary, line = string(line[0:end+1]), line[end+1:]
	} else {
		end := bytes.IndexByte(line, ' ')
		if end < 0 || len(line) <= end+1 {
			return blank, false
		}

		out.Summary, line = string(line[0:end]), line[end:]
	}

	// Now we can scan by word for a bit
	scanner := bufio.NewScanner(bytes.NewReader(line))
	scanner.Split(bufio.ScanWords)

	// From field
	if !scanner.Scan() {
		return blank, false
	}
	out.From = scanner.Text()

	// Hardcoded -> delimeter
	if !scanner.Scan() || !bytes.Equal(scanner.Bytes(), []byte("->")) {
		return blank, false
	}

	// To field
	if !scanner.Scan() {
		return blank, false
	}
	out.To = scanner.Text()

	// Reason field - optional, the rest of the line. This implementation will
	// squeeze multiple spaces into one, but that shouldn't be a big problem
	var reason []string
	for scanner.Scan() {
		reason = append(reason, scanner.Text())
	}
	out.Reason = strings.Join(reason, " ")

	return out, true
}
