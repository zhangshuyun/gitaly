package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &migrate.Migration{
		Id: "20200707101830_repositories_table",
		Up: []string{`
CREATE TABLE repository_generations (
	virtual_storage TEXT,
	relative_path TEXT,
	generation BIGINT NOT NULL,
	PRIMARY KEY (virtual_storage, relative_path)
);

CREATE TABLE storage_generations (
    virtual_storage TEXT,
    relative_path TEXT,
    storage TEXT,
    generation BIGINT NOT NULL,
    PRIMARY KEY (virtual_storage, relative_path, storage),
    FOREIGN KEY (virtual_storage, relative_path) REFERENCES repository_generations
);`},
		Down: []string{"DROP TABLE repository_generations; DROP TABLE storage_generations;"},
	}

	allMigrations = append(allMigrations, m)
}
