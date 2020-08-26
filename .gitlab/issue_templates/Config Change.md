# Gitaly/Praefect configuration change checklist

There are a number of projects which depend on Gitaly's or Praefect's configuration. When you change the configuration, please have a look whether the change needs to be propagated to any of the listed projects.

1. [ ] Update the example configuration files for [Gitaly](https://gitlab.com/gitlab-org/gitaly/-/blob/master/config.toml.example) and [Praefect](https://gitlab.com/gitlab-org/gitaly/-/blob/master/config.praefect.toml.example)
1. [ ] Update in [omnibus-gitlab](https://gitlab.com/gitlab-org/omnibus-gitlab) the configuration templates for [Gitaly](https://gitlab.com/gitlab-org/omnibus-gitlab/-/blob/master/files/gitlab-cookbooks/gitaly/templates/default/gitaly-config.toml.erb) and [Praefect](https://gitlab.com/gitlab-org/omnibus-gitlab/-/blob/master/files/gitlab-cookbooks/praefect/templates/default/praefect-config.toml.erb): `<MR LINK>`
1. [ ] Update in [gitlab-development-kit](https://gitlab.com/gitlab-org/gitlab-development-kit) the configuration templates for [Gitaly](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/master/support/templates/gitaly.config.toml.erb) and [Praefect](https://gitlab.com/gitlab-org/gitlab-development-kit/-/blob/master/support/templates/praefect.config.toml.erb): `<MR LINK>` 
1. [ ] Update in [GitLab Chart](https://gitlab.com/gitlab-org/charts/gitlab) the configuration template for [Gitaly](https://gitlab.com/gitlab-org/charts/gitlab/-/blob/master/charts/gitlab/charts/gitaly/templates/configmap.yml): `<MR LINK>`
1. [ ] Update in [CNG](https://gitlab.com/gitlab-org/build/CNG) the [development configuration for Gitaly](https://gitlab.com/gitlab-org/build/CNG/-/blob/master/dev/gitaly-config/config.toml) and the [default configuration for Gitaly](https://gitlab.com/gitlab-org/build/CNG/-/blob/master/gitaly/config.toml): `<MR LINK>`
1. [ ] Update [GitLab test setup](https://gitlab.com/gitlab-org/gitlab): `<MR LINK>`
1. [ ] Update [gitlab-elasticsearch-indexer](https://gitlab.com/gitlab-org/gitlab-elasticsearch-indexer): `<MR LINK>`
1. [ ] Update [gitlab-workhorse](https://gitlab.com/gitlab-org/gitlab-workhorse): `<MR LINK>`
