/title [Feature flag] Enable description of feature

## What

Enable the `:feature_name` feature flag ...

## Owners

- Team: Gitaly
- Most appropriate slack channel to reach out to: `#g_create_gitaly`
- Best individual to reach out to: NAME

## Expectations

### What release does this feature occur in first?

### What are we expecting to happen?

### What might happen if this goes wrong?

### What can we monitor to detect problems with this?

<!--

Which dashboards from https://dashboards.gitlab.net are most relevant?
Usually you'd just like a link to the method you're changing in the
dashboard at:

https://dashboards.gitlab.net/d/000000199/gitaly-feature-status

I.e.

1. Open that URL
2. Change "method" to your feature, e.g. UserDeleteTag
3. Copy/paste the URL & change gprd to gstd to monitor staging as well as prod

-->

## Roll Out Steps

- [ ] Enable on staging
    - [ ] Is the required code deployed on staging? ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#is-the-required-code-deployed))
    - [ ] Enable on staging ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#enable-on-staging))
    - [ ] Add ~"featureflag::staging" to this issue ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#feature-flag-labels))
    - [ ] Test on staging ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#test-on-staging))
    - [ ] Verify the feature flag was used by checking Prometheus metric [`gitaly_feature_flag_checks_total`](https://prometheus.gstg.gitlab.net/graph?g0.expr=sum%20by%20(flag)%20(rate(gitaly_feature_flag_checks_total%5B5m%5D))&g0.tab=1&g0.stacked=0&g0.range_input=1h)
- [ ] Enable on production
    - [ ] Is the required code deployed on production? ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#is-the-required-code-deployed))
    - [ ] Enable on production in `#production` ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#enable-in-production))
    - [ ] Add ~"featureflag::production" to this issue
    - [ ] Verify the feature flag was used by checking Prometheus metric [`gitaly_feature_flag_checks_total`](https://prometheus.gprd.gitlab.net/graph?g0.expr=sum%20by%20(flag)%20(rate(gitaly_feature_flag_checks_total%5B5m%5D))&g0.tab=1&g0.stacked=0&g0.range_input=1h)
- [ ] Default-enable the feature flag (optional, only required if backwards-compatibility concerns exist)
    - [ ] Wait for release containg default-disabled feature flag.
    - [ ] Change the feature flag to default-enabled ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#feature-lifecycle-after-it-is-live))
    - [ ] Wait for release containing default-enabled feature flag.
- [ ] Remove feature flag
    - [ ] Remove the feature flag and the pre-feature-flag code ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#feature-lifecycle-after-it-is-live))
    - [ ] Remove the feature flag via chatops ([howto](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#remove-the-feature-flag-via-chatops))
    - [ ] Close this issue

Please refer to the [documentation of feature flags](https://gitlab.com/gitlab-org/gitaly/-/blob/master/doc/PROCESS.md#feature-flags) for further information.

/label ~"devops::create" ~"group::gitaly" ~"feature flag" ~"feature::maintainance" ~"Category:Gitaly" ~"section::dev" ~"featureflag::disabled"
