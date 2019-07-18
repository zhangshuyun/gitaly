CREATE TABLE storages (
  name TEXT PRIMARY KEY
);

CREATE TABLE nodes (
  address TEXT PRIMARY KEY
);

CREATE TABLE storage_nodes (
  id SERIAL PRIMARY KEY,
  storage_name TEXT REFERENCES storages(name),
  node_address TEXT REFERENCES nodes(address)
);

CREATE TABLE repositories (
  relative_path TEXT PRIMARY KEY,
  "primary" INTEGER REFERENCES storage_nodes(id)
);

CREATE TABLE repository_secondaries (
  repository_relative_path TEXT REFERENCES repositories(relative_path),
  storage_node_id INTEGER REFERENCES storage_nodes(id),
  PRIMARY KEY(repository_relative_path, storage_node_id)
);

INSERT INTO storages ("name") VALUES ('default');
INSERT INTO storages ("name") VALUES ('backup1');
INSERT INTO storages ("name") VALUES ('backup2');

INSERT INTO nodes (address) VALUES('tcp://127.0.0.1:9999');
INSERT INTO nodes (address) VALUES('tcp://127.0.0.1:9998');

INSERT INTO storage_nodes (id, node_address, storage_name) VALUES(1, 'tcp://127.0.0.1:9999', 'default');
INSERT INTO storage_nodes (id, node_address, storage_name) VALUES(2, 'tcp://127.0.0.1:9998', 'default');
INSERT INTO storage_nodes (id, node_address, storage_name) VALUES(3, 'tcp://127.0.0.1:9999', 'backup1');
INSERT INTO storage_nodes (id, node_address, storage_name) VALUES(4, 'tcp://127.0.0.1:9998', 'backup2');

INSERT INTO repositories(relative_path, "primary")
  VALUES('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 1);

INSERT INTO repository_secondaries(repository_relative_path, storage_node_id) VALUES
  ('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 2),
  ('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 3),
  ('@hashed/2c/62/2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3.git', 4);