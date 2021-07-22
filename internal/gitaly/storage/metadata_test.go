package storage

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func readFilesystemID(t *testing.T, path string) string {
	metadata := make(map[string]string)

	f, err := os.Open(filepath.Join(path, metadataFilename))
	require.NoError(t, err)
	defer f.Close()

	require.NoError(t, json.NewDecoder(f).Decode(&metadata))
	return metadata["gitaly_filesystem_id"]
}

func TestWriteMetdataFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tempDir))
	}()

	require.NoError(t, WriteMetadataFile(tempDir))
	require.NotEmpty(t, readFilesystemID(t, tempDir))
}

func TestWriteMetadataFile_AlreadyExists(t *testing.T) {
	tempDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tempDir))
	}()

	metadataPath := filepath.Join(tempDir, ".gitaly-metadata")
	metadataFile, err := os.Create(metadataPath)
	require.NoError(t, err)

	m := Metadata{
		GitalyFilesystemID: uuid.New().String(),
	}

	require.NoError(t, json.NewEncoder(metadataFile).Encode(&m))
	require.NoError(t, metadataFile.Close())

	require.NoError(t, WriteMetadataFile(tempDir))

	require.Equal(t, m.GitalyFilesystemID, readFilesystemID(t, tempDir), "WriteMetadataFile should not clobber the existing file")
}

func TestReadMetadataFile(t *testing.T) {
	metadata, err := ReadMetadataFile("testdata")
	require.NoError(t, err)
	require.Equal(t, "test filesystem id", metadata.GitalyFilesystemID, "filesystem id should match the harded value in testdata/.gitaly-metadata")
}

func TestReadMetadataFile_FileNotExists(t *testing.T) {
	_, err := ReadMetadataFile("/path/doesnt/exist")
	require.Error(t, err)
}
