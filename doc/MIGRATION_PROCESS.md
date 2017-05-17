[All current migrations](https://gitlab.com/gitlab-org/gitaly/issues?label_name%5B%5D=Migration&scope=all&state=all)

# Gitaly Migration Process

This document describes the Gitaly migration process.

Migration is done on a feature-by-feature basis. Although not strictly correct, for simplicity, think of each migrated feature as being a single Gitaly endpoint.

![](https://docs.google.com/drawings/d/1wPwweMnEUPgffsdmoVmHwho5wzKtj61ll_Q90N9eZc8/pub?w=756&h=720)

[Edit this document](https://docs.google.com/drawings/d/1wPwweMnEUPgffsdmoVmHwho5wzKtj61ll_Q90N9eZc8/edit)

---------------------------------------------------------------------

## Selection

First step is to consider a route for migration. A route is an endpoint in one of the following GitLab projects:

* GitLab-CE
* GitLab Workhorse
* GitLab Shell

The order of migration is roughly determined using the formula described in the [Order of Migration](../README.md#order-of-migration) section in the Gitaly readme, although tactical and strategic reasons may affect the actual order.

---------------------------------------------------------------------

## Migration Analysis: ~"Migration Analysis"  

Once a route has been selected, the route in the client will be analysed and profiled in order to figure out the best way of migrating the route to Gitaly.

The artefacts of this stage will be:

1. **Rough estimation of the amount of work involved in the migration**: from the analysis, we should have a rough idea of how long this migration will take.
1. **Decision to move ahead with migration**: At this stage of the project, we're working to achieve the best value for effort. If we feel that a migration will take too much effort for the value gained, then it may be shelved.
1. **Optional: a new grpc Endpoint**: the analysis may show that the route can be migrated using an existing Gitaly endpoint, or that a new endpoint needs to be designed. If an existing endpoint is used, jump directly to **Client Implementation**, skipping **RPC design** and **Server Implementation**

---------------------------------------------------------------------

## RPC Design: ~"RPC Design"

A new GRPC endpoint is added to the [`gitaly-proto`](https://gitlab.com/gitlab-org/gitaly-proto) project.

---------------------------------------------------------------------

## Server Implementation: ~"Server Implementation"

The server implementation of the `gitaly-proto` endpoint is completed, including:
* Unit tests
* Integration tests

Note: the client and server implementations may occur in parallel, or sequentially
depending on the particular case.

---------------------------------------------------------------------

## Client Implementation: ~"Client Implementation"

The client implementation in `gitlab-ce`, `GitLab-Workhorse` or `GitLab-Shell` is completed.



#### Feature Flags

The client code will either call the old route or the new route, depending on a **feature flag**. This flag name should be derived from the name of the grpc endpoint.

---------------------------------------------------------------------

## Feature Status: *Ready-for-Testing*

At this stage, the feature will be considered to complete, but should remain
disabled by default until acceptance testing has been completed.

This happens in three stages:
* Feature Status: Ready-for-Testing
* Feature Status Opt-In
* Feature Status: Opt-Out
* Feature Status: Mandatory

---------------------------------------------------------------------

## Acceptance Testing: ~"Acceptance Testing"

A feature is tested in dev, staging and gitlab.com. If the results are satisfactory, the testing continues on to the next environment until the test is complete.

The following procedure should be used for testing:

1. Update the relevant [chef roles](https://dev.gitlab.org/cookbooks/chef-repo) to enable the feature flag(s) under `default_attributes -> omnibus-gitlab -> gitlab_rb -> gitlab-rails -> env`:
  - For dev: roles/dev-gitlab-org.json
  - For staging: roles/gitlab-staging-base.json
  - For producion: roles/gitlab-cluster-base.json
1. Create a new row in the Gitaly dashboard to monitor the feature in the [`gitaly-dashboards`](https://gitlab.com/gitlab-org/gitaly-dashboards) repo.
  - Merge the chef-repo MRs
  - Make sure new role file is on chef server `bundle exec knife role from file [role-path]`
  - Run chef client and restart unicorn: `bundle exec knife ssh -C 1 role:[role-name] 'sudo chef-client; sleep 60; sudo gitlab-ctl term unicorn; echo done $?'`
  - Verify the process have the env values set: `bundle exec knife ssh role:[role-name] 'for p in $(pgrep -f "unicorn master"); do ps -o pid,etime,args -p $p; sudo cat  /proc/$p/environ | xargs --null --max-args=1 | grep GITALY; done'`
1. Restart client process (unicorn, workhorse, etc) if necessary to enable the feature.
1. Monitor dashboards and host systems to ensure that feature is working.
1. Get the production engineer to roll the feature back.
1. Review data:
    1. Did the test route perform well?
    1. Did the client or server processes consume excessive resources during the test?
    1. Did error rates jump during the test?
1. If the test if successful, proceed to next environment.

Once acceptance testing has been successfully completed in all three environments, we need to prepare for opt-in status.

* The [Gitaly runbook](https://gitlab.com/gitlab-com/runbooks/blob/master/troubleshooting/gitaly-error-rate.md) should be updated to include any diagnosis information and a description of how to disable the feature flag.
* Using the error-rate data from the gitlab.com acceptance testing, new alerts need to be added to the [Gitaly prometheus rules](https://gitlab.com/gitlab-com/runbooks/blob/master/alerts/gitaly.rules). The alert should also include a link to the new runbook amendments.

---------------------------------------------------------------------

## Feature Status: *Opt-In*

As the maintainer of Gitaly, Jacob to review:

* The testing evidence completed in the acceptance testing stage is sufficient, using the dashboards created in the previous stage.
* The alerts are in-place
* The runbooks are good

Once Jacob has approved, the feature flag will be enabled on dev.gitlab.org, staging and GitLab.com, but the feature flag will be disabled by default. On-premise installations can enable the feature if they wish, but it will be disabled by default.

For a feature toggle `GITALY_EXAMPLE_FEATURE`, the toggle would be enabled by setting the environment variable:  

```shell
GITALY_EXAMPLE_FEATURE=1 # "One" to enable
```

If the flag is missing, the feature will be **disabled by default**.

---------------------------------------------------------------------

## Feature Status: *Opt-Out*

In the next GitLab release, the client application logic will be switched around to make the feature toggle enabled by default.

This gives on-premise installations a month to test a feature and disable it if there are any problems.

Disabling a feature is done by:

For a feature toggle `GITALY_EXAMPLE_FEATURE`, the toggle would be enabled by setting the environment variable:  

```shell
GITALY_EXAMPLE_FEATURE=0 # "Zero" to disable
```

If the flag is missing, the feature will be **enabled by default**.

---------------------------------------------------------------------

## Feature Status: *Mandatory*

In the next GitLab release following the change to Opt-Out feature status, the feature will be made mandatory. At this point, the option to opt-out will be gone and all GitLab installations will need to use the feature.

The change will be made by:

* Removing the references to the feature flag in Omnibus, Chef repo, etc
* Remove the feature flag switching code from the client application (GitLab-CE, Workhorse, GitLab-Shell) code
