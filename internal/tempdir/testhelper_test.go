package tempdir

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

var cleanRoot string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()

	tempDir, err := ioutil.TempDir("", "gitaly-tests")
	if err != nil {
		log.Error(err)
		return 1
	}
	defer os.RemoveAll(tempDir)

	cleanRoot = filepath.Join(tempDir, tmpRootPrefix)

	return m.Run()
}
