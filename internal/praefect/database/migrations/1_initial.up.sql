CREATE TABLE IF NOT EXISTS nodes(
  id SERIAL PRIMARY KEY,
  storage TEXT NOT NULL,
  address TEXT NOT NULL,
  token TEXT
);

CREATE TABLE IF NOT EXISTS repositories(
  id SERIAL PRIMARY KEY,
  relative_path TEXT NOT NULL
);

CREATE TYPE node_type AS ENUM ('primary', 'secondary');

CREATE TABLE IF NOT EXISTS nodes_repositories(
  node_id INTEGER REFERENCES nodes(id),
  repository_id INTEGER REFERENCES repositories(id),
  type node_type,
  PRIMARY KEY (node_id, repository_id)
);