package archive

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

// TarBuilder writes a .tar archive to an io.Writer. The contents of the archive
// are determined by successive calls to `File` and `RecursiveDir`.
//
// If an error occurs during processing, all subsequent calls to TarWriter will
// fail with that same error. The same error will be returned by `Err()`.
//
// TarBuilder is **not** safe for concurrent use.
type TarBuilder struct {
	basePath  string
	tarWriter *tar.Writer

	// The first error stops all further processing
	err error
}

// NewTarBuilder creates a TarBuilder that writes files from basePath on the
// filesystem to the given io.Writer
func NewTarBuilder(basePath string, w io.Writer) *TarBuilder {
	return &TarBuilder{
		basePath:  basePath,
		tarWriter: tar.NewWriter(w),
	}
}

func (t *TarBuilder) join(rel string) string {
	return filepath.Join(t.basePath, rel)
}

func (t *TarBuilder) setErr(err error) error {
	t.err = err
	return err
}

func (t *TarBuilder) entry(fi os.FileInfo, filename string, r io.Reader) error {
	if !fi.Mode().IsRegular() && !fi.Mode().IsDir() {
		return fmt.Errorf("unsupported mode for %v: %v", filename, fi.Mode())
	}

	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return err
	}

	if fi.IsDir() && !strings.HasSuffix(filename, "/") {
		filename = filename + "/"
	}

	hdr.Name = filename

	if err := t.tarWriter.WriteHeader(hdr); err != nil {
		return err
	}

	if fi.Mode().IsRegular() {
		// Size is included in the tar header, so ensure exactly that many bytes
		// are written. This may lead to an inconsistent file with concurrent
		// writes, but the archive itself will be well-formed. Archive creation
		// will fail outright if the file is shortened.
		if _, err := io.CopyN(t.tarWriter, r, fi.Size()); err != nil {
			return err
		}
	}

	return nil
}

func (t *TarBuilder) walk(path string, fi os.FileInfo, err error) error {
	// Stop completely if an error is encountered walking the directory
	if err != nil {
		return err
	}

	// This condition strongly suggests an application bug
	rel, err := filepath.Rel(t.basePath, path)
	if err != nil {
		return err
	}

	if fi.Mode().IsDir() {
		return t.entry(fi, rel, nil)
	}

	// Ignore symlinks and special files in directories
	if !fi.Mode().IsRegular() {
		return nil
	}

	return t.File(rel, true)
}

// File writes a single regular file to the archive. It is an error if the file
// exists, but is not a regular file - including symlinks.
//
// If `mustExist` is set, an error is returned if the file doesn't exist.
// Otherwise, the error is hidden.
func (t *TarBuilder) File(rel string, mustExist bool) error {
	if t.err != nil {
		return t.err
	}

	filename := t.join(rel)

	// O_NOFOLLOW causes an error to be returned if the file is a symlink
	file, err := os.OpenFile(filename, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		// The file doesn't exist, but we've been told that's OK
		if os.IsNotExist(err) && !mustExist {
			return nil
		}

		// Halt in any other circumstance
		return t.setErr(err)
	}

	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return t.setErr(err)
	}

	return t.setErr(t.entry(fi, rel, file))
}

// RecursiveDir adds a complete directory to the archive, including all
// subdirectories and any regular files in the tree. Anything that is not a
// regular file (including symlinks, etc) will be **skipped**.
//
// If `mustExist` is true, an error is returned if the root directory doesn't
// exist. Otherwise, the error is hidden.
//
// If patterns is non-empty, only those matching files and directories will be
// included. Otherwise, all are included.
func (t *TarBuilder) RecursiveDir(rel string, mustExist bool, patterns ...*regexp.Regexp) error {
	if t.err != nil {
		return t.err
	}

	root := t.join(rel)

	if _, err := os.Lstat(root); err != nil {
		if os.IsNotExist(err) && !mustExist {
			return nil
		}

		return t.setErr(err)
	}

	walker := t.walk
	if len(patterns) > 0 {
		walker = matchWalker{wrapped: t.walk, patterns: patterns}.Walk
	}

	// Walk the root and its children, recursively
	return t.setErr(filepath.Walk(root, walker))
}

// FileIfExist is a helper for File that sets `mustExist` to false.
func (t *TarBuilder) FileIfExist(rel string) error {
	return t.File(rel, false)
}

// RecursiveDirIfExist is a helper for RecursiveDir that sets `mustExist` to
// false.
func (t *TarBuilder) RecursiveDirIfExist(rel string, patterns ...*regexp.Regexp) error {
	return t.RecursiveDir(rel, false, patterns...)
}

// Close finalizes the archive and releases any underlying resources. It should
// always be called, whether an error has been encountered in processing or not.
func (t *TarBuilder) Close() error {
	if t.err != nil {
		// Ignore any close error in favour of reporting the previous one, but
		// ensure the tar writer is closed to avoid resource leaks
		t.tarWriter.Close()
		return t.err
	}

	return t.tarWriter.Close()
}

// Err returns the last error seen during operation of a TarBuilder. Once an
// error has been encountered, the TarBuilder will cease further operations. It
// is safe to make a series of calls, then just check `Err()` at the end.
func (t *TarBuilder) Err() error {
	return t.err
}
