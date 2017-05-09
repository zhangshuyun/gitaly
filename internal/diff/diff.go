package diff

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

const blankID = "0000000000000000000000000000000000000000"

// Diff represents a single parsed diff entry
type Diff struct {
	FromID    string
	ToID      string
	OldMode   int32
	NewMode   int32
	FromPath  []byte
	ToPath    []byte
	Binary    bool
	Status    byte
	RawChunks [][]byte
}

// Parser holds necessary state for parsing a diff stream
type Parser struct {
	patchReader       *bufio.Reader
	rawLines          [][]byte
	currentDiff       *Diff
	nextPatchFromPath []byte
	finished          bool
	err               error
}

var (
	rawLineRegexp    = regexp.MustCompile(`(?m)^:(\d+) (\d+) ([[:xdigit:]]{40}) ([[:xdigit:]]{40}) ([ADTUXMRC]\d*)\t(.*?)(?:\t(.*?))?$`)
	diffHeaderRegexp = regexp.MustCompile(`(?m)^diff --git "?a/(.*?)"? "?b/(.*?)"?$`)
)

// NewDiffParser returns a new Parser
func NewDiffParser(src io.Reader) *Parser {
	parser := &Parser{}
	reader := bufio.NewReader(src)

	parser.cacheRawLines(reader)
	parser.patchReader = reader

	return parser
}

// Parse parses a single diff. It returns true if successful, false if it finished
// parsing all diffs or when it encounters an error, in which case use Parser.Err()
// to get the error.
func (parser *Parser) Parse() bool {
	if parser.finished || len(parser.rawLines) == 0 {
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

	for {
		// We cannot use bufio.Scanner because the line may be very long.
		line, err := parser.patchReader.Peek(10)
		if err == io.EOF {
			parser.finished = true

			if parser.currentDiff == nil { // Probably NewDiffParser was passed an empty src (e.g. git diff failed)
				return false
			}

			if len(line) > 0 && len(line) < 10 {
				consumeChunkLine(parser.patchReader, parser.currentDiff)
			}

			return true
		} else if err != nil {
			parser.err = fmt.Errorf("peek diff line: %v", err)
			return false
		}

		if bytes.HasPrefix(line, []byte("diff --git")) {
			return true
		} else if bytes.HasPrefix(line, []byte("Binary")) {
			parser.err = consumeBinaryNotice(parser.patchReader, parser.currentDiff)
		} else if bytes.HasPrefix(line, []byte("@@")) {
			parser.currentDiff.RawChunks = append(parser.currentDiff.RawChunks, nil)

			parser.err = consumeChunkLine(parser.patchReader, parser.currentDiff)
		} else if helper.ByteSliceHasAnyPrefix(line, "---", "+++") {
			parser.err = consumeLine(parser.patchReader)
		} else if helper.ByteSliceHasAnyPrefix(line, "-", "+", " ", "\\") {
			parser.err = consumeChunkLine(parser.patchReader, parser.currentDiff)
		} else {
			parser.err = consumeLine(parser.patchReader)
		}

		if parser.err != nil {
			return false
		}
	}

	return true
}

// Diff returns a successfully parsed diff. It should be called only when Parser.Parse()
// returns true.
func (parser *Parser) Diff() *Diff {
	return parser.currentDiff
}

// Err returns the error encountered (if any) when parsing the diff stream. It should be called only when Parser.Parse()
// returns false.
func (parser *Parser) Err() error {
	return parser.err
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
	// Raw and regular diff formats don't necessarily have the same files, since some flags (e.g. --ignore-space-change)
	// can suppress certain kinds of diffs from showing in regular format, but raw format will always have all the files.
	parser.currentDiff = &Diff{}
	if err := parseRawLine(parser.nextRawLine(), parser.currentDiff); err != nil {
		parser.err = err
		return err
	}
	if parser.currentDiff.Status == 'T' {
		parser.handleTypeChangeDiff()
	}

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
		blankID,
		parser.currentDiff.ToID,
		parser.currentDiff.FromPath,
	)

	parser.currentDiff.NewMode = 0
	parser.currentDiff.ToID = blankID

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

func consumeChunkLine(reader *bufio.Reader, diff *Diff) error {
	line, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("read chunk line: %v", err)
	}

	chunkIndex := len(diff.RawChunks) - 1
	diff.RawChunks[chunkIndex] = append(diff.RawChunks[chunkIndex], line...)

	return nil
}

func consumeBinaryNotice(reader *bufio.Reader, diff *Diff) error {
	_, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("read binary notice: %v", err)
	}

	diff.Binary = true

	return nil
}

func consumeLine(reader *bufio.Reader) error {
	_, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("read line: %v", err)
	}

	return nil
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
