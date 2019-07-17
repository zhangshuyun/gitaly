CREATE TABLE IF NOT EXISTS node_storages (
  id SERIAL PRIMARY KEY,
  address TEXT NOT NULL,
  storage_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shards (
  relative_path TEXT PRIMARY KEY,
  "primary" INTEGER REFERENCES node_storages(id)
);

CREATE TABLE IF NOT EXISTS shard_secondaries (
  shard_relative_path TEXT REFERENCES shards(relative_path),
  node_storage_id INTEGER REFERENCES node_storages(id),
  PRIMARY KEY(shard_relative_path, node_storage_id)
);
