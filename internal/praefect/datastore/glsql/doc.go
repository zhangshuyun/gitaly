// Package glsql provides integration with SQL database. It contains a set
// of functions and structures that help to interact with SQL database and
// to write tests to check it.

// A simple unit tests do not require any additional dependencies except mocks.
// The tests to check interaction with a database require database to be up and
// running.
// You must provide PGHOST and PGPORT environment variables to run the tests.
// PGHOST - is a host of the Postgres database to connect to.
// PGPORT - is a port which is used by Postgres database to listen for incoming
// connections.
//
// If you use Gitaly inside GDK or want to reuse Postgres instance from GDK
// navigate to gitaly folder from GDK root and run command:
// $ gdk env
// it should print PGHOST and PGPORT for you.

// To check if everything configured properly run the command:
//
// $ PGHOST=<host of db instance> \
//   PGPORT=<port of db instance> \
//   go test \
//    -tags=postgres \
//    -v \
//    -count=1 \
//    gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql \
//    -run=^TestOpenDB$
//
// Once it is finished successfully you can be sure other tests would be able to
// connect to the database and interact with it.
//
// As you may note there is a special build tag `postgres` in the `go` command
// above. This build tag distinguishes tests that depend on the database from those
// which are not. When adding a new tests with a dependency to database please add
// this build tag to them. The example how to do this could be found in
// internal/praefect/datastore/glsql/postgres_test.go file.

// To simplify usage of transactions the TxQuery interface was introduced.
// The main idea is to write code that won't be overwhelmed with transaction
// management and to use simple approach with OK/NOT OK check while running
// SQL queries using transaction. Let's take a look at the usage example:
//
// let's imagine we have this method and db is *sql.DB on the repository struct:
// func (r *repository) Save(ctx context.Context, u User) (err error) {
// 	// initialization of our new transactional scope
// 	txq := NewTxQuery(ctx, nil, r.db)
// 	// call for Done is required otherwise transaction will remain open
// 	// err must be not a nil value. In this case it is a reference to the
//	// returned named parameter. It will be filled with error returned from Exec
//	// func call if any and no other Exec function call will be triggered.
// 	defer txq.Done(&err)
//	// the first operation is attempt to insert a new row
//	// in case of failure the error would be propagated into &err passed to Done method
//	// and it will return false that will be a signal that operation failed or was not
//	// triggered at all because there was already a failed operation on this transaction.
// 	userAdded := txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
// 		_, err := tx.Exec(ctx, "INSERT INTO user(name) VALUES ($1)", u.Name)
// 		return err
// 	})
//	// we can use checks for early return, but if there was an error on the previous operation
//	// the next one won't be executed.
// 	if !userAdded {
// 		return
// 	}
// 	txq.Exec(ctx, func(ctx context.Context, tx *sql.Tx) error {
// 		_, err := tx.Exec(ctx, "UPDATE stats SET user_count = user_count + 1")
// 		return err
// 	})
// }
//
// NOTE: because we use [pgbouncer](https://www.pgbouncer.org/) with transaction pooling
// it is [not allowed to use prepared statements](https://www.pgbouncer.org/faq.html).

package glsql
