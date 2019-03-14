/*Package transaction provides transaction management functionality to
coordinate one-to-many clients attempting to modify the shards concurrently.

While Git is distributed in nature, there are some repository wide data points
that can conflict between replicas if something goes wrong. This includes
references, which is why the transaction manager provides an API that allows
an RPC transaction to read/write lock the references being accessed to prevent
contention.
*/
package transaction
