gitaly['enable'] = true
gitaly['listen_addr'] = ':6666'

# This instance will be serving the 'gitaly2' repository storage
git_data_dirs({
  'gitaly2' => '/var/opt/gitlab/git-data-2'
})

# Disable as many Omnibus services as we can
unicorn['enable'] = false
sidekiq['enable'] = false
gitlab_workhorse['enable'] = false
gitlab_monitor['enable'] = false
prometheus_monitoring['enable'] = false
redis['enable'] = false
postgresql['enable']=false
nginx['enable'] = false

# We need these settings to prevent Omnibus from erroring out because
# Postgres/Redis are unavailable
gitlab_rails['rake_cache_clear'] = false
gitlab_rails['auto_migrate'] = false

