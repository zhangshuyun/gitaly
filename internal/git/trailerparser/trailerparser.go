package trailerparser

import (
	"bytes"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// The maximum number of trailers that we'll parse.
	maxTrailers = 64

	// The maximum size (in bytes) of trailer keys
	//
	// The limit should be sufficient enough to support multi-byte trailer keys
	// (or just long trailer keys).
	maxKeySize = 128

	// The maximum size (in bytes) of trailer values.
	//
	// This limit should be sufficient enough to support URLs, long Email
	// addresses, etc.
	maxValueSize = 512
)

// Parse parses Git trailers into a list of CommitTrailer objects.
//
// The expected input is a single line containing trailers in the following
// format:
//
//     KEY:VALUE\0KEY:VALUE
//
// Where \0 is a NULL byte. The input should not end in a NULL byte.
//
// Trailers must be separated with a null byte, as their values can include any
// other separater character. NULL bytes however are not allowed in commit
// messages, and thus can't occur in trailers.
//
// The key-value separator must be a colon, as this is the separator the Git CLI
// uses when obtaining the trailers of a commit message.
//
// Trailer keys and values are limited to a certain number of bytes. If these
// limits are reached, parsing stops and all trailers parsed until that point
// are returned. This ensures we don't continue to consume a potentially very
// large input.
//
// The limits this parser imposes on the sizes/amounts are loosely based on
// trailers found in GitLab's own repository. Here are just a few examples:
//
//     Change-Id: I009c716ce2475b9efa3fd07aee9215fca7a1c150
//     Changelog: https://github.com/nahi/httpclient/blob/b51d7a8bb78f71726b08fbda5abfb900d627569f/CHANGELOG.md#changes-in-282
//     Co-Authored-By: Alex Kalderimis <akalderimis@gitlab.com>
//     fixes: https://gitlab.com/gitlab-org/gitlab-ce/issues/44458
//     Signed-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>
//     See: https://gitlab.com/gitlab-org/gitlab-ee/blob/ff9ad690650c23665439499d23f3ed22103b55bb/spec/spec_helper.rb#L50
func Parse(input []byte) []*gitalypb.CommitTrailer {
	// The choice of a nil slice instead of an empty one is deliberate: gRPC
	// turns empty slices into nil.
	//
	// If we were to use an empty array, then compare that with a commit
	// retrieved using e.g. the Ruby server, we'd be comparing different types:
	// the Ruby server would produce a nil for an empty list of trailers, while
	// Gitaly would produce an empty slice. Using a nil slice here ensures these
	// type differences don't occur.
	var trailers []*gitalypb.CommitTrailer
	startPos := 0
	max := len(input)

	for startPos < max && len(trailers) < maxTrailers {
		endPos := bytes.IndexByte(input[startPos:], byte('\000'))

		// endPos starts as an index relative to the start position, so we need
		// to convert this to an absolute index before we use it for slicing.
		if endPos >= 0 {
			endPos = startPos + endPos
		} else {
			endPos = max
		}

		sepPos := bytes.IndexByte(input[startPos:endPos], byte(':'))

		if sepPos > 0 {
			key := input[startPos : startPos+sepPos]
			value := bytes.TrimSpace(input[startPos+sepPos+1 : endPos])

			if len(key) > maxKeySize || len(value) > maxValueSize {
				break
			}

			trailers = append(trailers, &gitalypb.CommitTrailer{
				Key:   key,
				Value: value,
			})
		}

		startPos = endPos + 1
	}

	return trailers
}
