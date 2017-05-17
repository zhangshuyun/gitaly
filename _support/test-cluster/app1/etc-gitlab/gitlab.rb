# This is not strictly necessary but it makes the logs a little 'quieter'
prometheus_monitoring['enable'] = false

# Disable the local Gitaly instance
gitaly['enable'] = false

# Don't disable Gitaly altogether
gitlab_rails['gitaly_enabled'] = true

git_data_dirs({
  'default' => {'path' => '/mnt/data1', 'gitaly_address' => 'tcp://gitaly1:6666'},
  'gitaly2' => {'path' => '/mnt/data2', 'gitaly_address' => 'tcp://gitaly2:6666'},
})
