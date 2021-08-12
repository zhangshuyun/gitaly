package datastore

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

func getDB(t testing.TB) glsql.DB { return glsql.GetDB(t) }
