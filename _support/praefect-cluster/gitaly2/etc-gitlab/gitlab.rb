gitaly['enable'] = true
gitaly['listen_addr'] = ':6667'

# This instance will be serving the 'gitaly2' repository storage
git_data_dirs({
  'gitaly2' => { 'path' => '/var/opt/gitlab/git-data-2' }
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

# Use the remote internal API
gitlab_rails['internal_api_url'] = 'http://app1:80'

# We have to use the same token in all hosts for internal API authentication
gitlab_shell['secret_token'] = 'f4kef1xedt0ken'
