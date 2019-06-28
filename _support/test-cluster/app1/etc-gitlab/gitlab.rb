# This is not strictly necessary but it makes the logs a little 'quieter'
prometheus_monitoring['enable'] = false

# Disable the local Gitaly instance
gitaly['enable'] = false

# Don't disable Gitaly altogether
gitlab_rails['gitaly_enabled'] = true

redis['port'] = 6379
redis['bind'] = '0.0.0.0'

git_data_dirs({
  'default' => { 'path' => '/mnt/data1', 'gitaly_address' => 'tcp://gitaly1:6666' },
  'gitaly2' => { 'path' => '/mnt/data2', 'gitaly_address' => 'tcp://gitaly2:6667' }
})

# We have to use the same token in all hosts for internal API authentication
gitlab_shell['secret_token'] = 'f4kef1xedt0ken'
