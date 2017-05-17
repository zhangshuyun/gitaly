~Conversation: #XXX (*complete this*)

See the [Migration Process documentation](https://gitlab.com/gitlab-org/gitaly/blob/master/doc/MIGRATION_PROCESS.md#acceptance-testing-acceptance-testing) 
for more information on the Acceptance Testing stage of the process.

Feature Toggle Environment Variable: `XXXXXXXXXXXXXXX`

--------------------------------------------------------------------------------

- [ ] [Chef recipes](https://dev.gitlab.org/cookbooks/chef-repo) to enable/disable this feature (link to MR)
- [ ] [Grafana dashboard](https://gitlab.com/gitlab-org/gitaly-dashboards) for monitoring (link to MR)
- [ ] Environments
    - [ ] `dev.gitlab.org`
    - [ ] Staging
    - [ ] `gitlab.com`
- [ ] Test Results (post as comments on this issue)
    - [ ] Did the migration perform as expected? 
    - [ ] Did the code have reasonable performance characteristics?
    - [ ] Did error rates jump to an unacceptable level?
- [ ] Have the changes been rolled back pending final review?
- [ ] Runbook Added (link to MR)
- [ ] Prometheus Alerts Added (link to MR)