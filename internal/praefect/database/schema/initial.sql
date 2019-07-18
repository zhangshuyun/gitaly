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

INSERT INTO node_storages (id, address, storage_name) VALUES(1, 'tcp://127.0.0.1:9999', 'default');
INSERT INTO node_storages (id, address, storage_name) VALUES(2, 'tcp://127.0.0.1:9998', 'default');
INSERT INTO node_storages (id, address, storage_name) VALUES(3, 'tcp://127.0.0.1:9999', 'backup1');
INSERT INTO node_storages (id, address, storage_name) VALUES(4, 'tcp://127.0.0.1:9998', 'backup2');

INSERT INTO shards(relative_path, "primary")
  VALUES('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 1);

INSERT INTO shard_secondaries(shard_relative_path, storage_node_id) VALUES
  ('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 2),
  ('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 3),
  ('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 4);