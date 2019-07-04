INSERT INTO nodes(id, storage, address, token) VALUES(1, "default", "tcp://gitaly-primary:9999");
INSERT INTO nodes(id, storage, address, token) VALUES(2, "backup-1", "tcp://gitaly-backup-1:9999");
INSERT INTO nodes(id, storage, address, token) VALUES(3, "backup-2", "tcp://gitaly-backup-2:9999");

INSERT INTO repositories(id, relative_path) VALUES(1, "@hashed/3f/db/3fdba35f04dc8c462986c992bcf875546257113072a909c162f7e470e581e278.git");

INSERT INTO nodes_repositories(repository_id, node_id) VALUES(1, 2);
INSERT INTO nodes_repositories(repository_id, node_id) VALUES(1, 3);