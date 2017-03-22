## GitLab Release [GITLAB_RELEASE_VERSION] Checklist

### General

- [ ] Cut new Gitaly release
- [ ] Update [`GITALY_SERVER_VERSION`](https://gitlab.com/gitlab-org/gitlab-ce/blob/master/GITALY_SERVER_VERSION) in GitLab-CE if appropriate

### Infrastructure Changes

- [ ] Create appropriate issues and MRs in the [Infrastructure](https://gitlab.com/gitlab-com/infrastructure) repo for any infrastructure changes for this release.

**Infrastructure Issues:**
* ...
* ...

### Monitoring & Alerting

- [ ] Update dashboards in Grafana with new metrics for this release
- [ ] Review new alerts for this release

### Documentation

- [ ] Update [Gitaly `README.md`](https://gitlab.com/gitlab-org/gitaly/blob/master/README.md) if appropriate
- [ ] Source Install Documentation
- [ ] Support Documentation
- [ ] Update `docs.gitlab.com`
- [ ] Prepare release post for [about.gitlab.com](https://gitlab.com/gitlab-com/www-gitlab-com/merge_requests?label_name%5B%5D=blog+post&label_name%5B%5D=release&scope=all&state=opened)
- [ ] Announce new updates in the [backend weekly call](https://docs.google.com/document/d/1psnkmByLkLh_39WtqvHCvj252HRhPGK8Fe3eQP_Yjf8/edit)

### Omnibus

- [ ] Create appropriate issues and MRs in the [omnibus-gitlab](https://gitlab.com/gitlab-org/omnibus-gitlab) repo

**Omnibus Issues:**
* ...
* ...

### Feedback Loop

- [ ] Update `.gitlab/issue_templates/GitLab_Release.md` with changes from this release.
