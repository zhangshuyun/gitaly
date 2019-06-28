# Test cluster with multiple Gitaly servers

This directory contains a
[docker-compose.yml](https://docs.docker.com/compose/) and Omnibus
GitLab configuration files to boot GitLab with multiple Gitaly servers
behind it. This setup is meant for testing purposes only and SHOULD NOT be used
in production environments because it handles secrets in an unsafe way.

Boot the cluster with `docker-compose up`. After some time you can log
in to your GitLab instance at `localhost:8080`.

This template uses nightly docker images. To see what GitLab version you are
currently running, run
`docker-compose exec app1 grep gitlab-ce /opt/gitlab/version-manifest.txt`. To
update to the latest nightly images run `docker-compose pull`.
