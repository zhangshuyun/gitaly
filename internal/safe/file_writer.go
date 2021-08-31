package safe

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// ErrAlreadyDone is returned when the safe file has already been closed
// or committed
var ErrAlreadyDone = errors.New("safe file was already committed or closed")

// FileWriter is a thread safe writer that does an atomic write to the target file. It allows one
// writer at a time to acquire a lock, write the file, and atomically replace the contents of the target file.
type FileWriter struct {
	tmpFile       *os.File
	path          string
	commitOrClose sync.Once
}

// CreateFileWriter takes path as an absolute path of the target file and creates a new FileWriter by attempting to create a tempfile
func CreateFileWriter(path string) (*FileWriter, error) {
	writer := &FileWriter{path: path}

	directory := filepath.Dir(path)

	tmpFile, err := ioutil.TempFile(directory, filepath.Base(path))
	if err != nil {
		return nil, err
	}

	writer.tmpFile = tmpFile

	return writer, nil
}

// Write wraps the temporary file's Write.
func (fw *FileWriter) Write(p []byte) (n int, err error) {
	return fw.tmpFile.Write(p)
}

// Commit will close the temporary file and rename it to the target file name
// the first call to Commit() will close and delete the temporary file, so
// subsequently calls to Commit() are gauaranteed to return an error.
func (fw *FileWriter) Commit() error {
	err := ErrAlreadyDone

	fw.commitOrClose.Do(func() {
		if err = fw.tmpFile.Sync(); err != nil {
			err = fmt.Errorf("syncing temp file: %v", err)
			return
		}

		if err = fw.tmpFile.Close(); err != nil {
			err = fmt.Errorf("closing temp file: %v", err)
			return
		}

		if err = fw.rename(); err != nil {
			err = fmt.Errorf("renaming temp file: %v", err)
			return
		}

		if err = fw.syncDir(); err != nil {
			err = fmt.Errorf("syncing dir: %v", err)
			return
		}
	})

	return err
}

// rename renames the temporary file to the target file
func (fw *FileWriter) rename() error {
	return os.Rename(fw.tmpFile.Name(), fw.path)
}

// syncDir will sync the directory
func (fw *FileWriter) syncDir() error {
	f, err := os.Open(filepath.Dir(fw.path))
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Sync()
}

// Close will close and remove the temp file artifact if it exists. If the file
// was already committed, an ErrAlreadyClosed error will be returned and no
// changes will be made to the filesystem.
func (fw *FileWriter) Close() error {
	err := ErrAlreadyDone

	fw.commitOrClose.Do(func() {
		if err = fw.tmpFile.Close(); err != nil {
			return
		}
		if err = os.Remove(fw.tmpFile.Name()); err != nil && !os.IsNotExist(err) {
			return
		}
	})

	return err
}
