package diff

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
)

// Diff represents a single parsed diff entry
type Diff struct {
	Binary         bool
	OverflowMarker bool
	Collapsed      bool
	TooLarge       bool
	Status         byte
	lineCount      int
	FromID         string
	ToID           string
	OldMode        int32
	NewMode        int32
	FromPath       []byte
	ToPath         []byte
	Patch          []byte
}

// Reset clears all fields of d in a way that lets the underlying memory
// allocations of the []byte fields be reused.
func (d *Diff) Reset() {
	*d = Diff{
		FromPath: d.FromPath[:0],
		ToPath:   d.ToPath[:0],
		Patch:    d.Patch[:0],
	}
}

// Parser holds necessary state for parsing a diff stream
type Parser struct {
	limits            Limits
	patchReader       *bufio.Reader
	rawLines          [][]byte
	currentDiff       Diff
	nextPatchFromPath []byte
	filesProcessed    int
	linesProcessed    int
	bytesProcessed    int
	finished          bool
	err               error
}

// Limits holds the limits at which either parsing stops or patches are collapsed
type Limits struct {
	// If true, Max{Files,Lines,Bytes} will cause parsing to stop if any of these limits is reached
	EnforceLimits bool
	// If true, SafeMax{Files,Lines,Bytes} will cause diffs to collapse (i.e. patches are emptied) after any of these limits reached
	CollapseDiffs bool
	// Number of maximum files to parse. The file parsed after this limit is reached is marked as the overflow.
	MaxFiles int
	// Number of diffs lines to parse (including lines preceded with --- or +++).
	// The file in which this limit is reached is discarded and marked as the overflow.
	MaxLines int
	// Number of bytes to parse (including lines preceded with --- or +++).
	// The file in which this limit is reached is discarded and marked as the overflow.
	MaxBytes int
	// Number of files to parse, after which all subsequent files are collapsed.
	SafeMaxFiles int
	// Number of lines to parse (including lines preceded with --- or +++), after which all subsequent files are collapsed.
	SafeMaxLines int
	// Number of bytes to parse (including lines preceded with --- or +++), after which all subsequent files are collapsed.
	SafeMaxBytes int
	// Number of bytes a single patch can have. Patches surpassing this limit are pruned / nullified.
	MaxPatchBytes int
}

const (
	// maxFilesUpperBound controls how much MaxFiles limit can reach
	maxFilesUpperBound = 5000
	// maxLinesUpperBound controls how much MaxLines limit can reach
	maxLinesUpperBound = 250000
	// maxBytesUpperBound controls how much MaxBytes limit can reach
	maxBytesUpperBound = 5000 * 5120 // 24MB
	// safeMaxFilesUpperBound controls how much SafeMaxBytes limit can reach
	safeMaxFilesUpperBound = 500
	// safeMaxLinesUpperBound controls how much SafeMaxLines limit can reach
	safeMaxLinesUpperBound = 25000
	// safeMaxBytesUpperBound controls how much SafeMaxBytes limit can reach
	safeMaxBytesUpperBound = 500 * 5120 // 2.4MB
	// maxPatchBytesUpperBound controls how much MaxPatchBytes limit can reach
	maxPatchBytesUpperBound = 512000 // 500KB
)

var (
	rawLineRegexp    = regexp.MustCompile(`(?m)^:(\d+) (\d+) ([[:xdigit:]]{40}) ([[:xdigit:]]{40}) ([ADTUXMRC]\d*)\t(.*?)(?:\t(.*?))?$`)
	diffHeaderRegexp = regexp.MustCompile(`(?m)^diff --git "?a/(.*?)"? "?b/(.*?)"?$`)
)

// NewDiffParser returns a new Parser
func NewDiffParser(src io.Reader, limits Limits) *Parser {
	limits.enforceUpperBound()

	parser := &Parser{}
	reader := bufio.NewReader(src)

	parser.cacheRawLines(reader)
	parser.patchReader = reader
	parser.limits = limits

	return parser
}

// Parse parses a single diff. It returns true if successful, false if it finished
// parsing all diffs or when it encounters an error, in which case use Parser.Err()
// to get the error.
func (parser *Parser) Parse() bool {
	if parser.finished || len(parser.rawLines) == 0 {
		// In case we didn't consume the whole output due to reaching limitations
		io.Copy(ioutil.Discard, parser.patchReader)
		return false
	}

	if err := parser.initializeCurrentDiff(); err != nil {
		return false
	}

	if err := parser.findNextPatchFromPath(); err != nil {
		return false
	}

	if !bytes.Equal(parser.nextPatchFromPath, parser.currentDiff.FromPath) {
		// The current diff has an empty patch
		return true
	}

	// We are consuming this patch so it is no longer 'next'
	parser.nextPatchFromPath = nil

	for currentPatchDone := false; !currentPatchDone || parser.patchReader.Buffered() > 0; {
		// We cannot use bufio.Scanner because the line may be very long.
		line, err := parser.patchReader.Peek(10)
		if err == io.EOF {
			parser.finished = true
			currentPatchDone = true
		} else if err != nil {
			parser.err = fmt.Errorf("peek diff line: %v", err)
			return false
		}

		if bytes.HasPrefix(line, []byte("diff --git")) {
			break
		} else if bytes.HasPrefix(line, []byte("@@")) {
			parser.consumeChunkLine(false)
		} else if helper.ByteSliceHasAnyPrefix(line, "---", "+++") && !parser.isParsingChunkLines() {
			parser.consumeLine(false)
		} else if bytes.HasPrefix(line, []byte("~\n")) {
			parser.consumeChunkLine(true)
		} else if bytes.HasPrefix(line, []byte("Binary")) {
			parser.currentDiff.Binary = true
			parser.consumeChunkLine(true)
		} else if helper.ByteSliceHasAnyPrefix(line, "-", "+", " ", "\\") {
			parser.consumeChunkLine(true)
		} else {
			parser.consumeLine(false)
		}

		if parser.err != nil {
			return false
		}
	}

	if parser.limits.CollapseDiffs && parser.isOverSafeLimits() && parser.currentDiff.lineCount > 0 {
		parser.prunePatch()
		parser.currentDiff.Collapsed = true
	}

	if parser.limits.EnforceLimits {
		// Apply single-file size limit
		if len(parser.currentDiff.Patch) >= parser.limits.MaxPatchBytes {
			parser.prunePatch()
			parser.currentDiff.TooLarge = true
		}

		maxFilesExceeded := parser.filesProcessed > parser.limits.MaxFiles
		maxBytesOrLinesExceeded := parser.bytesProcessed >= parser.limits.MaxBytes || parser.linesProcessed >= parser.limits.MaxLines

		if maxFilesExceeded || maxBytesOrLinesExceeded {
			parser.finished = true
			parser.currentDiff.Reset()
			parser.currentDiff.OverflowMarker = true
		}
	}

	return true
}

// enforceUpperBound ensures every limit value is within its corresponding upperbound
func (limit *Limits) enforceUpperBound() {
	limit.MaxFiles = min(limit.MaxFiles, maxFilesUpperBound)
	limit.MaxLines = min(limit.MaxLines, maxLinesUpperBound)
	limit.MaxBytes = min(limit.MaxBytes, maxBytesUpperBound)
	limit.SafeMaxFiles = min(limit.SafeMaxFiles, safeMaxFilesUpperBound)
	limit.SafeMaxLines = min(limit.SafeMaxLines, safeMaxLinesUpperBound)
	limit.SafeMaxBytes = min(limit.SafeMaxBytes, safeMaxBytesUpperBound)
	limit.MaxPatchBytes = min(limit.MaxPatchBytes, maxPatchBytesUpperBound)
}

// prunePatch nullifies the current diff patch and reduce lines and bytes processed
// according to it.
func (parser *Parser) prunePatch() {
	parser.linesProcessed -= parser.currentDiff.lineCount
	parser.bytesProcessed -= len(parser.currentDiff.Patch)
	// Clear Patch, but preserve underlying memory allocation
	parser.currentDiff.Patch = parser.currentDiff.Patch[:0]
}

// Diff returns a successfully parsed diff. It should be called only when Parser.Parse()
// returns true. The return value is valid only until the next call to Parser.Parse().
func (parser *Parser) Diff() *Diff {
	return &parser.currentDiff
}

// Err returns the error encountered (if any) when parsing the diff stream. It should be called only when Parser.Parse()
// returns false.
func (parser *Parser) Err() error {
	return parser.err
}

func (parser *Parser) isOverSafeLimits() bool {
	return parser.filesProcessed > parser.limits.SafeMaxFiles ||
		parser.linesProcessed > parser.limits.SafeMaxLines ||
		parser.bytesProcessed > parser.limits.SafeMaxBytes
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (parser *Parser) cacheRawLines(reader *bufio.Reader) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				parser.err = err
				parser.finished = true
			}
			// According to the documentation of bufio.Reader.ReadBytes we cannot get
			// both an error and a line ending in '\n'. So the current line cannot be
			// valid and we want to discard it.
			return
		}

		if !bytes.HasPrefix(line, []byte(":")) {
			// Discard the current line and stop reading: we expect a blank line
			// between raw lines and patch data.
			return
		}

		parser.rawLines = append(parser.rawLines, line)
	}
}

func (parser *Parser) nextRawLine() []byte {
	if len(parser.rawLines) == 0 {
		return nil
	}

	line := parser.rawLines[0]
	parser.rawLines = parser.rawLines[1:]

	return line
}

func (parser *Parser) initializeCurrentDiff() error {
	parser.currentDiff.Reset()

	// Raw and regular diff formats don't necessarily have the same files, since some flags (e.g. --ignore-space-change)
	// can suppress certain kinds of diffs from showing in regular format, but raw format will always have all the files.
	if err := parseRawLine(parser.nextRawLine(), &parser.currentDiff); err != nil {
		parser.err = err
		return err
	}
	if parser.currentDiff.Status == 'T' {
		parser.handleTypeChangeDiff()
	}

	parser.filesProcessed++

	return nil
}

func (parser *Parser) findNextPatchFromPath() error {
	// Since we can only go forward when reading from a bufio.Reader, we save the matched FromPath we parsed until we
	// reach its counterpart in raw diff.
	if parser.nextPatchFromPath != nil {
		return nil
	}

	line, err := parser.patchReader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		parser.err = fmt.Errorf("read diff header line: %v", err)
		return parser.err
	} else if err == io.EOF {
		return nil
	}

	if matches := diffHeaderRegexp.FindSubmatch(line); len(matches) > 0 {
		parser.nextPatchFromPath = unescape(matches[1])
		return nil
	}

	parser.err = fmt.Errorf("diff header regexp mismatch")
	return parser.err
}

func (parser *Parser) handleTypeChangeDiff() {
	// GitLab wants to display the type change in the current diff as a removal followed by an addition.
	// To make this happen we add a new raw line, which will become the addition on the next iteration of the parser.
	// We change the current diff in-place so that it becomes a deletion.
	newRawLine := fmt.Sprintf(
		":%o %o %s %s A\t%s\n",
		0,
		parser.currentDiff.NewMode,
		git.ZeroOID,
		parser.currentDiff.ToID,
		parser.currentDiff.FromPath,
	)

	parser.currentDiff.NewMode = 0
	parser.currentDiff.ToID = git.ZeroOID.String()

	parser.rawLines = append([][]byte{[]byte(newRawLine)}, parser.rawLines...)
}

func parseRawLine(line []byte, diff *Diff) error {
	matches := rawLineRegexp.FindSubmatch(line)
	if len(matches) == 0 {
		return fmt.Errorf("raw line regexp mismatch")
	}

	mode, err := strconv.ParseInt(string(matches[1]), 8, 0)
	if err != nil {
		return fmt.Errorf("raw old mode: %v", err)
	}
	diff.OldMode = int32(mode)

	mode, err = strconv.ParseInt(string(matches[2]), 8, 0)
	if err != nil {
		return fmt.Errorf("raw new mode: %v", err)
	}
	diff.NewMode = int32(mode)

	diff.FromID = string(matches[3])
	diff.ToID = string(matches[4])
	diff.Status = matches[5][0]

	diff.FromPath = unescape(helper.UnquoteBytes(matches[6]))
	if diff.Status == 'C' || diff.Status == 'R' {
		diff.ToPath = unescape(helper.UnquoteBytes(matches[7]))
	} else {
		diff.ToPath = diff.FromPath
	}

	return nil
}

func (parser *Parser) consumeChunkLine(updateLineStats bool) {
	var line []byte
	var err error

	// The code that follows would be much simpler if we used
	// bufio.Reader.ReadBytes, but that allocates an intermediate copy of
	// each line which adds up to a lot of allocations. By using ReadSlice we
	// can copy bytes into currentDiff.Patch without intermediate
	// allocations.
	n := 0
	for done := false; !done; {
		line, err = parser.patchReader.ReadSlice('\n')
		n += len(line)

		switch err {
		case io.EOF, nil:
			done = true
		case bufio.ErrBufferFull:
			// long line: keep reading
		default:
			parser.err = fmt.Errorf("read chunk line: %v", err)
			return
		}

		parser.currentDiff.Patch = append(parser.currentDiff.Patch, line...)
	}

	if updateLineStats {
		parser.bytesProcessed += n
		parser.currentDiff.lineCount++
		parser.linesProcessed++
	}
}

func (parser *Parser) consumeLine(updateStats bool) {
	line, err := parser.patchReader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		parser.err = fmt.Errorf("read line: %v", err)
		return
	}

	if updateStats {
		parser.currentDiff.lineCount++
		parser.linesProcessed++
		parser.bytesProcessed += len(line)
	}
}

func (parser *Parser) isParsingChunkLines() bool {
	return len(parser.currentDiff.Patch) > 0
}

// unescape unescapes the escape codes used by 'git diff'
func unescape(s []byte) []byte {
	var unescaped []byte

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			if i+3 < len(s) && helper.IsNumber(s[i+1:i+4]) {
				octalByte, err := strconv.ParseUint(string(s[i+1:i+4]), 8, 8)
				if err == nil {
					unescaped = append(unescaped, byte(octalByte))

					i += 3
					continue
				}
			}

			if i+1 < len(s) {
				var unescapedByte byte

				switch s[i+1] {
				case '"', '\\', '/', '\'':
					unescapedByte = s[i+1]
				case 'b':
					unescapedByte = '\b'
				case 'f':
					unescapedByte = '\f'
				case 'n':
					unescapedByte = '\n'
				case 'r':
					unescapedByte = '\r'
				case 't':
					unescapedByte = '\t'
				default:
					unescaped = append(unescaped, '\\')
					unescapedByte = s[i+1]
				}

				unescaped = append(unescaped, unescapedByte)
				i++
				continue
			}
		}

		unescaped = append(unescaped, s[i])
	}

	return unescaped
}
