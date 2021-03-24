<!--- Replace Date in title below -->

/title Demo YYYY-MM-DD

<!--
## Contributing

When adding new feature demonstrations to the script, follow these guidelines.

For each feature you are verifying, add an H3 section with a link to the issue
to the `## Features` section.

Always add new features near the bottom of this section. This way older issues
will float to the top and allow them to be prioritized during the demo.

Make sure you break down steps into the following sections:

1. prep steps - these are steps needed to correctly set up your demonstration.
   These steps are okay for the demo runner to perform before the start of the
   demo call.
1. demo steps - these are the steps to perform during the demo call to show
   how the feature works
1. verify steps - these are the expected observations required to be seen
   in order to verify the prep or feature works as expected

Ideally, all setup steps should before the exercise steps (when possible).
Demo and verification steps may interleave as needed. For example, the
following structure is okay:

1. Prep
1. Prep
1. Verify
1. Prep
1. Demo
1. Verify
1. Demo
1. Demo
1. Verify
1. Verify

Along with the H3 section, it might look like this:

```
### #1234

1. [ ] Prep: install thingy
1. [ ] Verify: thingy works
1. [ ] Prep: turn on gizmo
1. [ ] Demo: press red button
1. [ ] Verify: world should explode
```

When your feature passes all verification steps, submit an MR to remove
it from this issue template.

-->

This issue is used to conduct a demo for exhibiting and verifying new behavior
for Gitaly and Praefect. Before the demo, run all `Prep:` steps. During the
demo, run through all remaining `Demo:` and `Verify` steps. Check each
step as completed or verified. Do not check a `Verify:` step if it does not
succeed.

## General Setup

1. [ ] Prep:
   - [ ] Check the [latest version of this issue template](https://gitlab.com/gitlab-org/gitaly/-/blob/master/.gitlab/issue_templates/Demo.md)
   for any new steps and update this issue accordingly.
   - [ ] Checkout the latest changes from Gitaly's default branch
   - [ ] `cd _support/terraform`
   - [ ] `./create-demo-cluster`
   - [ ] `./configure-demo-cluster`
   - [ ] Sign in as admin user `root` during the demo
   - [ ] Create a new repository on the GitLab instance
   - [ ] Log into the GitLab web interface and upload license

## Features

### Variable Replication Factor #2971

Previously Praefect has replicated repositories to every node in a virtual storage. This has made large clusters
unfeasible due to increasing cost of replicating repositories to every storage within a virtual storage. This
also made it impossible to horizontally scale a virtual storage's storage capacity. The virtual storage's storage
capacity would be limited by the smallest storage in the virtual storage as it has to fit every repository.

Variable replication factor allows administrator to set each repository's replication factor individually. This allows
for scaling the storage capacity of the cluster horizontally by allowing a repository's replication factor to be lower
than the storage count in a virtual storage. For important or highly used repositories, administrators can distribute
requests and increase redundancy by setting a higher replication factor.

Variable replication factor is only implemented using repository specific primaries. This is due to the primary node
needing a copy of each repository. Needing to have every repository on a single node would make the single primary a
bottleneck as it would need to contain every repository.

While variable replication factor itself is mostly ready, repository specific primaries still have some issues to solve.
Importantly for the demo, repository creation does not yet work. To work around that limitation, the prep step uses
`sql` elector.

1. Prep (all operations done on a Praefect node):
   - [ ] Create two repositories in the demo cluster. `sql` elector must be enabled while doing this due to the
         `per_repository` elector not being able to create repositories yet. These will be referred to as repository
         A and repository B later.
   - [ ] Run `sudo -i` as `gitlab-ctl` commands need to be run as root.
   - [ ] Connect to Postgres in another terminal by running `/opt/gitlab/embedded/bin/psql -U praefect -d praefect_production -h <postgres address>` on a Praefect node.
   - [ ] Ensure there are entries for every storage for both repositories in the `storage_repositories` table and
         that they are all on the same generation.
   - [ ] Enable repository specific primaries by setting `praefect['failover_election_strategy'] = 'per_repository'` in `/etc/gitlab/gitlab.rb` on Praefect nodes.
   - [ ] Disable the reconciler initially. Set `praefect['reconciliation_scheduling_interval'] = 0` in `/etc/gitlab/gitlab.rb` on Praefect nodes.
   - [ ] Reconfigure and restart the Praefect nodes by running `gitlab-ctl reconfigure`.
1. Demo:
   - [ ] Attempt to set replication factor 0 for repository A by running `/opt/gitlab/embedded/bin/praefect -config /var/opt/gitlab/praefect/config.toml set-replication-factor -virtual-storage default -repository <relative path A> -replication-factor 0`. This should fail as the minimum replication factor is 0.
   - [ ] Attempt to set replication factor 4 for repository A. This should fail as the demo cluster only has 3 storage nodes, meaning it is not possible to reach replication factor of 4.
   - [ ] Set replication factor of 1 for repository A. The command should print out the assigned storage. The assigned storage should be the repository's primary. You can verify this by running `SELECT * FROM repositories WHERE relative_path = '<relative path A>';` and checking the `primary` column refers to the same storage.
   - [ ] With the replication factor 1 set for repository A, perform a write in repository B. Repository B should still replicate on every node. After the write, you can verify each of the storages of B are on the same generation by running `SELECT * FROM storage_repositories WHERE relative_path = '<relative path B>';` and checking that the generations match.
   - [ ] Check the virtual storage's status with dataloss by running `/opt/gitlab/embedded/bin/praefect -config /var/opt/gitlab/praefect/config.toml dataloss -virtual-storage default -partially-replicated`. It should not list any outdated repositories.
   - [ ] Perform a write in repository A.
   - [ ] Check the virtual storage's status with dataloss again. Everything should be fully up to date as all the assigned storages are up to date. Check repository A's entries in the `storage_repositories` table and verify only the assigned node's generation was incremented. Only the assigned nodes participate in a transaction or get a replication job scheduled for a given write.
   - [ ] Set the replication factor of repository A to 2. You should observe a random secondary being assigned.
   - [ ] Check the virtual storage's status with dataloss again. It should now list repository A as one of the assigned storages is outdated. You should also see the other secondary listed outdated, but without being designated as assigned.
   - [ ] Perform another write in repository A.
   - [ ] Check the virtual storage's status with dataloss again. Repository A should not be listed anymore as the new write scheduled a replication job to bring the outdated assigned secondary back to speed.
   - [ ] Set the replication factor of repository A to 1 and perform a write.
   - [ ] Set the replication factor of repository A back to 2.
   - [ ] Check with dataloss that the assigned secondary is listed as outdated. The other secondary should also be listed outdated but not assigned.
   - [ ] Enable the reconciler by setting the `reconciliation_scheduling_interval` to '5s'. Reconfigure and restart Praefects.
   - [ ] Wait for the reconciler to schedule and execute the replication jobs.
   - [ ] Check with dataloss that repository A is no longer considered outdated. The reconciler only targets assigned nodes. Verify from the `storage_repositories` table that the unassigned storage is still on a lower generation than the assigned nodes.
   - [ ] Repeatedly increase and decrease the replication factor of repository A from 1 to 3 and back. You should observe the primary node is never unassigned, only the secondaries.

## After Demo

1. [ ] Create any follow up issues discovered during the demo and assign label
   ~demo.
   - Link the issues as related to this issue
1. [ ] [Follow teardown instructions to remove demo
   resources](https://gitlab.com/gitlab-org/gitaly/-/blob/master/_support/terraform/README.md#destroying-a-demo-cluster)
1. [ ] Open a new demo issue and assign to the next demo conductor
1. [ ] Close this issue

/label ~demo ~"group::gitaly" ~"devops::create"
