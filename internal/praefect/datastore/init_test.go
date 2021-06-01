// +build postgres

package datastore

import (
	"log"
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

func TestMain(m *testing.M) {
	code := m.Run()
	// Clean closes connection to database once all tests are done
	if err := glsql.Clean(); err != nil {
		log.Fatalln(err, "database disconnection failure")
	}
	os.Exit(code)
}

const databaseName = "datastore"

func getDB(t testing.TB) glsql.DB { return glsql.GetDB(t, databaseName) }

func getDBConfig(t testing.TB) config.DB { return glsql.GetDBConfig(t, databaseName) }
