~Conversation: #XXX (*complete this*)

See the [Migration Process documentation](https://gitlab.com/gitlab-org/gitaly/blob/master/doc/MIGRATION_PROCESS.md#acceptance-testing-acceptance-testing)
for more information on the Acceptance Testing stage of the process.

Feature Toggle Name: `gitaly_xxxxx`

Required Gitaly Version: `vX.X.X`

Required GitLab Version: `vX.X`

--------------------------------------------------------------------------------

## 1. Preparation

- [ ] **Routes**: what routes use this migration?
  - Please list a set of routes that are known to use this endpoint
- [ ] **Sentry**:
  - [ ] Ensure that all `gitaly_migrate` issues in the `GitLab.com` tracker are either assigned or resolved: https://sentry.gitlap.com/gitlab/gitlabcom/?query=is%3Aunresolved+is%3Aunassigned+gitaly_migrate
  - [ ] Ensure that all issues in the `Gitaly Production` tracker are either assigned or resolved: https://sentry.gitlap.com/gitlab/gitaly-production/?query=is%3Aunresolved+is%3Aunassigned
- [ ] **Grafana**
  - [ ] Link to the Gitaly Feature Status dashboard (edit accordingly): https://performance.gitlab.net/dashboard/db/gitaly-feature-status?var-method=CommitStats&refresh=5m&orgId=1
- [ ] **Kibana**
  - [ ] Based on routes listed above, provide a Kibana short-url link to incoming requests to that route. Use this example (for `/:group/:project/commits`) as a template: https://log.gitlap.com/goto/e789c1efc8bafaba6a4a4289093529a8
  - [ ] Provide a Kibana short-url link to Gitaly logs related to this endpoint

## 2. Development Trial

- [ ] **Check Dev Server Versions**: Ensure that the environments have the correct Gitaly and GitLab versions running
  - [ ] Gitaly: https://performance.gitlab.net/dashboard/db/gitaly-version-tracker?orgId=1&var-job=gitaly-dev
  - [ ] GitLab: https://dev.gitlab.org/help

- [ ] Enable on `dev.gitlab.org`:
  - [ ] `!feature-set gitaly_FEATURE_NAME 1` in [`#dev-gitlab`](https://gitlab.slack.com/messages/C6WQ87MU3)
  - [ ] Inspect dev logs in ELK (base dev search: https://log.gitlap.com/goto/9dc4f7767fc8305778ad6a99efe186c0)
  - [ ] Check for errors in Gitaly Sentry: https://sentry.gitlap.com/gitlab/devgitlaborg-gitaly/
  - [ ] Check for errors in dev.gitlab.org Sentry: https://sentry.gitlap.com/gitlab/devgitlaborg/?query=is%3Aunresolved+gitaly
  - [ ] Perform some testing and leave the feature enabled

## 3. Staging Trial

- [ ] **Check Staging Server Versions**: Ensure that the environments have the correct Gitaly and GitLab versions running
  - [ ] Gitaly: https://performance.gitlab.net/dashboard/db/gitaly-version-tracker?orgId=1&var-job=gitaly-staging
  - [ ] GitLab: https://staging.gitlab.com/help

- [ ] Enable on `staging.gitlab.com` in [`#development`](https://gitlab.slack.com/messages/C02PF508L/)
  - [ ] Perform some testing and leave the feature enabled

## 4. Production Server Version Check

- [ ] Gitaly: https://performance.gitlab.net/dashboard/db/gitaly-version-tracker?orgId=1&var-job=gitaly-production
- [ ] GitLab: https://gitlab.com/help

## 5. Initial Impact Check

- [ ] Set Gitaly to 1% using the command `!feature-set gitaly_FEATURE_NAME 1` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)
- [ ] Leave running for at least 15 minutes
- Monitor sentry, grafana and kibaba links above, every 5 minutes but preferably in "real-time"
  - [ ] On unusual activity, disable trial with `!feature-set gitaly_FEATURE_NAME false` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)

## 6. Low Impact Trial

- [ ] Set Gitaly to 5% using the command `!feature-set gitaly_FEATURE_NAME 5` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)
- [ ] Leave running for at least 2 hours
- Monitor sentry, grafana and kibaba links above, every 30 minutes
  - [ ] On unusual activity, disable trial with `!feature-set gitaly_FEATURE_NAME false` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)

## 7. Mid Impact Trial

- [ ] Set Gitaly to 50% using the command `!feature-set gitaly_FEATURE_NAME 50` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)
- [ ] Leave running for at least 24 hours
- Monitor sentry, grafana and kibaba links above, every few hours
  - [ ] On unusual activity, disable trial with `!feature-set gitaly_FEATURE_NAME false` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)


## 8. Full Impact Trial

- [ ] Set Gitaly to 100% using the command `!feature-set gitaly_FEATURE_NAME 100` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)
- [ ] Leave running for at least a week
- Monitor sentry, grafana and kibaba links above daily
  - [ ] On unusual activity, disable trial with `!feature-set gitaly_FEATURE_NAME false` in [`#production`](https://gitlab.slack.com/messages/C101F3796/)


## 9. Outcome

  - Report on the outcome of the tests (leave comments on this issue)
    - [ ] Did the migration perform as expected?
    - [ ] Did the code have reasonable performance characteristics?
    - [ ] Did error rates jump to an unacceptable level?

  - [ ] Additional Runbook information required?
    - [ ] If so, was it added? (link to MR)
  - [ ] Prometheus Alerts Added
    - [ ] If so, was it added? (link to MR)

/label ~"Acceptance Testing"
