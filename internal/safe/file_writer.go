package safe

import (
	"errors"
	"fmt"
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

// FileWriterConfig contains configuration for the `NewFileWriter()` function.
type FileWriterConfig struct {
	// FileMode is the desired file mode of the committed target file. If left at its default
	// value, then no file mode will be explicitly set for the file.
	FileMode os.FileMode
}

// NewFileWriter takes path as an absolute path of the target file and creates a new FileWriter by
// attempting to create a tempfile. This function either takes no FileWriterConfig or exactly one.
func NewFileWriter(path string, optionalCfg ...FileWriterConfig) (*FileWriter, error) {
	var cfg FileWriterConfig
	if len(optionalCfg) == 1 {
		cfg = optionalCfg[0]
	} else if len(optionalCfg) > 1 {
		return nil, fmt.Errorf("file writer created with more than one config")
	}

	writer := &FileWriter{path: path}

	directory := filepath.Dir(path)

	tmpFile, err := os.CreateTemp(directory, filepath.Base(path))
	if err != nil {
		return nil, err
	}

	if cfg.FileMode != 0 {
		if err := tmpFile.Chmod(cfg.FileMode); err != nil {
			_ = writer.Close()
			return nil, err
		}
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
