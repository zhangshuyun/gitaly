package lines

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"regexp"
)

// SenderOpts contains fields that Send() uses to determine what is considered
// a line, and how to handle pagination. That is, how many lines to skip, before
// a line gets fed into the Sender.
type SenderOpts struct {
	// Delimiter is the separator used to split the sender's output into
	// lines. Defaults to an empty byte (0).
	Delimiter byte
	// Limit is the upper limit of how many lines will be sent. The zero
	// value will cause no lines to be sent.
	Limit int
	// IsPageToken allows control over which results are sent as part of the
	// response. When IsPageToken evaluates to true for the first time,
	// results will start to be sent as part of the response. This function
	// will	be called with an empty slice previous to sending the first line
	// in order to allow sending everything right from the beginning.
	IsPageToken func([]byte) bool
	// When PageTokenError is true than Sender will return an error when provided
	// PageToken is not found.
	PageTokenError bool
	// Filter limits sent results to those that pass the filter. The zero
	// value (nil) disables filtering.
	Filter *regexp.Regexp
}

// ItemsPerMessage establishes the threshold to flush the buffer when using the
// `Send` function. It's a variable instead of a constant to make it possible to
// override in tests.
var ItemsPerMessage = 20

// Sender handles a buffer of lines from a Git command
type Sender func([][]byte) error

type writer struct {
	sender  Sender
	lines   [][]byte
	options SenderOpts
}

// CopyAndAppend adds a newly allocated copy of `e` to the `s` slice. Useful to
// avoid io buffer shennanigans
func CopyAndAppend(s [][]byte, e []byte) [][]byte {
	line := make([]byte, len(e))
	copy(line, e)
	return append(s, line)
}

// flush calls the `sender` handler function with the accumulated lines and
// clears the lines buffer.
func (w *writer) flush() error {
	if len(w.lines) == 0 { // No message to send, just return
		return nil
	}

	if err := w.sender(w.lines); err != nil {
		return err
	}

	// Reset the message
	w.lines = nil

	return nil
}

// addLine adds a new line to the writer buffer, and flushes if the maximum
// size has been achieved
func (w *writer) addLine(p []byte) error {
	w.lines = CopyAndAppend(w.lines, p)

	if len(w.lines) >= ItemsPerMessage {
		return w.flush()
	}

	return nil
}

// consume reads from an `io.Reader` and writes each line to the buffer. It
// flushes after being done reading.
func (w *writer) consume(r io.Reader) error {
	buf := bufio.NewReader(r)

	// As `IsPageToken` will instruct us to send the _next_ line only, we
	// need to call it before the first iteration to allow for the case
	// where we want to send right from the beginning.
	pastPageToken := w.options.IsPageToken([]byte{})
	for i := 0; i < w.options.Limit; {
		var line []byte

		for {
			// delim can be multiple bytes, so we read till the end byte of it ...
			chunk, err := buf.ReadBytes(w.delimiter())
			if err != nil && err != io.EOF {
				return err
			}

			line = append(line, chunk...)
			// ... then we check if the last bytes of line are the same as delim
			if bytes.HasSuffix(line, []byte{w.delimiter()}) {
				break
			}

			if err == io.EOF {
				i = w.options.Limit // Implicit exit clause for the loop
				break
			}
		}

		line = bytes.TrimSuffix(line, []byte{w.delimiter()})
		if len(line) == 0 {
			break
		}

		// If a page token is given, we need to skip all lines until we've found it.
		// All remaining lines will then be sent until we reach the pagination limit.
		if !pastPageToken {
			pastPageToken = w.options.IsPageToken(line)
			continue
		}

		if w.filter() != nil && !w.filter().Match(line) {
			continue
		}
		i++ // Only increment the counter if the result wasn't skipped

		if err := w.addLine(line); err != nil {
			return err
		}
	}

	if !pastPageToken && w.options.PageTokenError {
		return fmt.Errorf("could not find page token")
	}

	return w.flush()
}

func (w *writer) delimiter() byte        { return w.options.Delimiter }
func (w *writer) filter() *regexp.Regexp { return w.options.Filter }

// Send reads output from `r`, splits it at `opts.Delimiter`, then handles the
// buffered lines using `sender`.
func Send(r io.Reader, sender Sender, opts SenderOpts) error {
	if opts.IsPageToken == nil {
		opts.IsPageToken = func(_ []byte) bool { return true }
	}

	writer := &writer{sender: sender, options: opts}
	return writer.consume(r)
}
