CREATE TABLE IF NOT EXISTS storage_nodes (
  id SERIAL PRIMARY KEY,
  storage TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repositories (
  id SERIAL PRIMARY KEY,
  relative_path TEXT NOT NULL,
  "primary" INTEGER REFERENCES storage_nodes (id),
);

CREATE TABLE IF NOT EXISTS repository_replicas (
  repository_id INTEGER REFERENCES repositories(id),
  storage_node_id INTEGER REFERENCES storage_nodes(id),
  PRIMARY KEY(repository_id, storage_node_id)
);