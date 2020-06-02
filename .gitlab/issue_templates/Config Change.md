# gitaly/praefect config change checklist

When you make a change to the config for Praefect or Gitaly there are a number of steps to ship the change.

- [ ] update [omnibus](https://gitlab.com/gitlab-org/omnibus-gitlab) MR LINK
- [ ] update [GDK](https://gitlab.com/gitlab-org/gitlab-development-kit) MR LINK
- [ ] update [gitaly config.toml in Charts](https://gitlab.com/gitlab-org/charts/gitlab/-/blob/master/charts/gitlab/charts/gitaly/templates/configmap.yml) MR LINK
- [ ] update [dev config.toml in CNG](https://gitlab.com/gitlab-org/build/CNG/-/blob/master/dev/gitaly-config/config.toml)  MR LINK
- [ ] update [Gitlab Test Setup](https://gitlab.com/gitlab-org/gitlab) MR LINK
