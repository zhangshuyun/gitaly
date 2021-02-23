## Object Pools

When creating forks of a repository, most of the objects for forked repository
and the repository it forked from are shared. Storing those shared objects
multiple times is a waste of disk space and also of CPU time, given that those
shared objects would have to be repacked for both repositories. To fix this
waste of resources, we use object pools, which are essentially a repository
which holds the shared objects of both repositories.

The sharing of objects for a given repository and its object pool is done via
alternate object directories which Gitaly sets up when linking a repository to
an object pool by writing the `objects/info/alternates` file.

### Lifetime of Object Pools

The lifetime of object pools is maintained via the
[ObjectPoolService](../proto/objectpool.proto), which provides various RPCs to
create and delete object pools as well as to add members to or remove members
from the pool.

An object pool is typically created from an existing repository by doing a
[`--local`](https://git-scm.com/docs/git-clone#Documentation/git-clone.txt---local)
clone of the repository, which bypasses the normal transport mechanisms and
instead simply performs a copy of the references and objects.

Afterwards, any repositories which shall be a member of the pool needs to be
linked to it. Linking most importantly involves setting up the "alternates" file
of the pool member, but it also includes deleting all bitmaps for packs of the
member. This is required by git because it can only ever use a single bitmap.
While it's not an error to have multiple bitmaps, git will print a [user-visible
warning](https://gitlab.com/gitlab-org/gitaly/-/issues/1728) on clone or fetch
if there are. See [git-multi-pack-index(1)](https://git-scm.com/docs/multi-pack-index#_future_work)
for an explanation of this limitation.

Removing a member from an object pool is slightly more involved, as members of
an object pool members will miss objects which are only part of the object pool.
It is thus not as simple as removing `objects/info/alternates`, as that would
leave behind a corrupt repository. Instead, Gitaly hard-links all objects which
are part of the object pool into the dissociating member first and removes the
alternate afterwards. In order to check whether the operation succeeded, Gitaly
now runs git-fsck(1) to check for missing objects. If there are none, the
dissociation has succeeded. Otherwise, it will fail and re-add the alternates
file.

### Housekeeping

Housekeeping for object pools is handled differently from normal repositories as
it not only involves repacking the pool, but also updating it. The houskeeping
task is thus hosted by the `FetchIntoObjectPool` RPC. This task is typically
only executed with the original object pool member from which the pool has been
seeded and updates the pool by fetching from that member.

It performs the following tasks:

1. Common housekeeping tasks are performed. These are common cleanups which are
   shared between object pools and normal repositories. Most importantly, it
   removes stale lockfiles and deletes known-broken stale references.

2. A fetch is performed from the object pool member into the object pool with a
   `+refs/*:refs/remotes/origin/*` refspec. This fetch is most notably not a
   pruning fetch, that is any reference which gets deleted in the member will
   stay around in the pool.

3. The fetch may create new dangling objects which are not referenced anymore in
   the pool repository. These dangling objects will be kept alive by creating
   dangling references such that they do not get deleted in the pool. See
   [Dangling Objects](#dangling-objects) for more information.

4. Loose references are packed via git-pack-refs(1).

5. The pool is repacked via git-repack(1). The repack produces a single packfile
   including all objects with a bitmap index. In order to improve reuse of
   packfiles where git will read data from the packfile directly instead of
   generating it on the fly, the packfile uses a delta island including
   `refs/heads` and `refs/tags`. This restricts git to only generate deltas for
   objects which are directly reachable via either a branch or a tag. Most
   notably, this causes us to not generate deltas against dangling references.

### Dangling Objects

When fetching from pool members into the object pool, then any force-updated
references may cause objects in the pool to not be referenced anymore. For
normal repositories, it is perfectly fine to delete those references after a
certain time. In the context of object pools, any other member of the pool may
still use any of those unreferenced objects. Deleting them would thus
potentially cause corrupt repositories.

This issue is kind of unsolvable: there is no point in time where it's safe to
delete objects from the object pool, as we do not know which repositories may be
linked to it. And even if we knew, we cannot determine all references of all
repositories at once in a race-free manner. We thus must consider each object to
still be referenced somewhere.

As a safeguard to not lose any objects by accident, we thus create dangling
references in the object pool after the fetch in `FetchIntoObjectPool`. For each
dangling object, a reference `refs/dangling/$OID` is created which points into
the object. This assures that each object is still referenced.

Having unreachable objects kept alive in this fashion does have its problems:

- For busy repositories, we generate loads of dangling references. While these
  references [cannot be seen by clients](#references), they are seen when
  performing housekeeping tasks on the object pool itself. Fetches into the
  object pool and repacking of references can thus become quite expensive.

- Keeping dangling references alive makes git consider them as reachable. While
  this is the exact effect we want to achieve, it will also cause git to
  generate packfiles which may use such objects as delta bases which would under
  normal circumstances be considered as unreachable. The resulting packfile is
  thus potentially suboptimal. Gitaly works around this issue by using a delta
  island for `refs/heads/` and `refs/tags/`. This can only be considered a
  best-effort strategy, as it only considers a single object pool member's
  reachability while ignoring potential reachability by any other pool member.

### References

When git repositories have alternates set up, then they by default advertise any
references of the alternate itself. A client would thus typically also see both
dangling references as well as any other reference which was potentially already
deleted in the pool member which the client is fetching from. Besides being
inefficient, the resulting references would also be wrong.

To avoid advertising of such references, Gitaly uses a workaround of setting the
config entry `core.alternateRefsCommand=exit 0 #`. This causes git to use the
given command instead of executing git-for-each-ref(1) in the alternate and thus
stops it from advertising alternate references.

### Further Reading

- [How Git object deduplication works in GitLab](https://docs.gitlab.com/ee/development/git_object_deduplication.html)
