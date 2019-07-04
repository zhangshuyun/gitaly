package database

import (
	"errors"
	"os"

	"github.com/go-pg/pg"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
)

type SQLDatastore struct {
	db *pg.DB
}

func NewSQLDatastore(addr string) *SQLDatastore {
	return &SQLDatastore{
		db: pg.Connect(&pg.Options{
			Addr:     addr,
			User:     os.Getenv("PRAEFECT_PG_USER"),
			Password: os.Getenv("PRAEFECT_PG_PASSWORD"),
			Database: os.Getenv("PRAEFECT_PG_DB"),
		})}
}

func (s *SQLDatastore) GetDefaultPrimary() (praefect.Node, error) {
	var node praefect.Node
	res, err := s.db.QueryOne(&node,
		"SELECT storage, address FROM nodes LIMIT 1",
		nil,
	)
	if err != nil {
		return node, err
	}

	if res.RowsAffected() == 0 {
		return node, errors.New("no node found")
	}

	return node, nil
}

func (s *SQLDatastore) GetDefaultPrimary() (praefect.Node, error) {
	var node praefect.Node
	res, err := s.db.QueryOne(&node,
		"SELECT storage, address FROM nodes LIMIT 1",
		nil,
	)
	if err != nil {
		return node, err
	}

	if res.RowsAffected() == 0 {
		return node, errors.New("no node found")
	}

	return node, nil
}

func (s *SQLDatastore) GetPrimary(repo Repository) (praefect.Node, error) {
	var node praefect.Node

	res, err := s.db.Model(&node).
		ColumnExpr("node.storage, node.address").
		Join("JOIN node_repositories on node.id = node_repositories.node_id").
		Join("JOIN repositories on node_repositories.repository_id = repositories.id").
		Where("repositories.relative_path = ?", repo.RelativePath).First()

	if err != nil {
		return node, err
	}

	if res.RowsAffected() == 0 {
		return node, errors.New("no node found")
	}

	return node, nil
}
