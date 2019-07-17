CREATE TABLE IF NOT EXISTS storage_nodes (
  id SERIAL PRIMARY KEY,
  storage TEXT NOT NULL,
  address TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shards (
  id SERIAL PRIMARY KEY,
  storage TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  "primary" INTEGER REFERENCES storage_nodes (id),
  UNIQUE (storage, relative_path)
);

CREATE TABLE IF NOT EXISTS shard_secondaries (
  shard_id INTEGER REFERENCES shards(id),
  storage_node_id INTEGER REFERENCES storage_nodes(id),
  PRIMARY KEY(shard_id, storage_node_id)
);