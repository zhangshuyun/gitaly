require 'socket'

require 'gitaly'
require 'spec_helper'

SOCKET_PATH = 'gitaly.socket'.freeze

module IntegrationClient
  def gitaly_stub(service)
    klass = Gitaly.const_get(service).const_get(:Stub)
    klass.new("unix:#{File.join(TMP_DIR, SOCKET_PATH)}", :this_channel_is_insecure)
  end

  def gitaly_repo(storage, relative_path)
    Gitaly::Repository.new(storage_name: storage, relative_path: relative_path)
  end
end

def start_gitaly
  build_dir = File.expand_path(File.join(GITALY_RUBY_DIR, '../_build'))
  GitlabShellHelper.setup_gitlab_shell

  config_toml = <<~CONFIG
    socket_path = "#{SOCKET_PATH}"
    bin_dir = "#{build_dir}/bin"

    [gitlab-shell]
    dir = "#{GITLAB_SHELL_DIR}"

    [gitaly-ruby]
    dir = "#{GITALY_RUBY_DIR}"

    [[storage]]
    name = "#{DEFAULT_STORAGE_NAME}"
    path = "#{DEFAULT_STORAGE_DIR}"
  CONFIG
  config_path = File.join(TMP_DIR, 'gitaly-rspec-config.toml')
  File.write(config_path, config_toml)

  test_log = File.join(TMP_DIR, 'gitaly-rspec-test.log')
  options = { out: test_log, err: test_log, chdir: TMP_DIR }

  gitaly_pid = spawn(File.join(build_dir, 'bin/gitaly'), config_path, options)
  at_exit { Process.kill('KILL', gitaly_pid) }

  wait_ready!(File.join(TMP_DIR, SOCKET_PATH))
end

def wait_ready!(socket)
  last_exception = StandardError.new('wait_ready! has not made any connection attempts')

  print('Booting gitaly for integration tests')
  100.times do |i|
    sleep 0.1
    printf('.')
    begin
      UNIXSocket.new(socket).close
      puts ' ok'
      return
    rescue => ex
      last_exception = ex
    end
  end

  puts
  raise last_exception
end

start_gitaly
