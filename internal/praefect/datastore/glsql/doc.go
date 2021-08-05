// Package glsql provides integration with SQL database. It contains a set
// of functions and structures that help to interact with SQL database and
// to write tests to check it.

// A simple unit tests do not require any additional dependencies except mocks.
// Some of the tests require a running Postgres database instance.
// You need to provide PGHOST, PGPORT and PGUSER environment variables to run the tests.
// PGHOST - is a host of the Postgres database to connect to.
// PGPORT - is a port which is used by Postgres database to listen for incoming
// connections.
// PGUSER - is a user of the Postgres database that needs to be used.
//
// If you use Gitaly inside GDK or want to reuse Postgres instance from GDK
// navigate to gitaly folder from GDK root and run command:
// $ gdk env
// it should print PGHOST and PGPORT for you.

// To check if everything configured properly run the command:
//
// $ PGHOST=<host of db instance> \
//   PGPORT=<port of db instance> \
//   PGUSER=postgres \
//   go test \
//    -v \
//    -count=1 \
//    gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql \
//    -run=^TestOpenDB$
//
// Once it is finished successfully you can be sure other tests would be able to
// connect to the database and interact with it.
//
// NOTE: because we use [pgbouncer](https://www.pgbouncer.org/) with transaction pooling
// it is [not allowed to use prepared statements](https://www.pgbouncer.org/faq.html).

package glsql
