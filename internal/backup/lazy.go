package backup

import (
	"bytes"
	"context"
	"io"
)

// LazyWrite saves all the data from the r by relativePath and will only create
// a file if there is data to be written.
func LazyWrite(ctx context.Context, sink Sink, relativePath string, r io.Reader) error {
	var buf [256]byte
	n, err := r.Read(buf[:])
	if err == io.EOF {
		if n == 0 {
			return nil
		}
	} else if err != nil {
		return err
	}
	r = io.MultiReader(bytes.NewReader(buf[:n]), r)
	return sink.Write(ctx, relativePath, r)
}
