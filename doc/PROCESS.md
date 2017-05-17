## Gitaly Team Process

Gitaly is a fairly unique service in GitLab in that is has no dependencies on [gitlab-rails](https://gitlab.com/gitlab-org/gitlab-ce) or its SQL database.

This means that we can iterate faster than the gitlab-rails project, adding improvements (particularly optimisations) at a faster rate.

### Gitaly Releases

![](https://docs.google.com/drawings/d/1TlvxINA7vVNru7r9FGtLumoLRUmGgwR673Gtsonowns/pub?w=960&h=720)
[Edit this diagram](https://docs.google.com/drawings/d/1TlvxINA7vVNru7r9FGtLumoLRUmGgwR673Gtsonowns/edit)

This release process will work, provided the intra-gitlab-release are *semver* *patch* releases that don't introduce breaking API changes.

The focus of these patch releases would be performance improvements. New functionality would be added in *semver minor* or *major* releases in lock-step with the GitLab release train.

### Iterative Process

![](https://docs.google.com/drawings/d/11KY4ef2A1w1cie_um-ROUJ1N3GyFuWwhNEHjCzglzbA/pub?w=1440&h=810)
[Edit this diagram](https://docs.google.com/drawings/d/11KY4ef2A1w1cie_um-ROUJ1N3GyFuWwhNEHjCzglzbA/edit)

The diagram explains most of the process.

* Two week long iterations, kickoff on a Monday
* Two milestones per iteration
* One to two releases per iteration

#### Integrating with the GitLab Release Cycle

The GitLab release cycle culminates in a monthly release on the 22nd of each month. The cut for this release currently happens on the 7th of the month. Since Gitaly will be using a shorter, two week fixed cycle, some planning will be needed to ensure that we have a new stable release ready for the cut-off date. This will happen at the iteration kick-off.

## Branching Model

Like other GitLab projects, Gitaly uses the [GitLab Workflow](https://docs.gitlab.com/ee/workflow/gitlab_flow.html)  branching model.

![](https://docs.google.com/drawings/d/1VBDeOouLohq5EqOrht_9IGgNGQ2D6WgW_O6TgKytU2w/pub?w=960&h=720)

[Edit this diagram](https://docs.google.com/a/gitlab.com/drawings/d/1VBDeOouLohq5EqOrht_9IGgNGQ2D6WgW_O6TgKytU2w/edit)

* Merge requests will target the master branch.
* If the merge request is an optimisation of the previous stable branch, i.e. the branch currently running on GitLab.com, the MR will be cherry picked across to the stable branch and deployed to Gitlab.com from there.

# Migration Process

The Gitaly team aim to migrate each feature across to Gitaly according to a standardised process - read about the [Gitaly Migration Process here](MIGRATION_PROCESS.md)