require 'gitaly'
require 'test_repo_helper'

SOCKET_PATH = 'gitaly.socket'.freeze
TMP_DIR = File.expand_path('../../tmp', __FILE__)

module IntegrationClient
  def gitaly_stub(service)
    klass = Gitaly.const_get(service).const_get(:Stub)
    klass.new("unix:tmp/#{SOCKET_PATH}", :this_channel_is_insecure)
  end

  def gitaly_repo(storage, relative_path)
    Gitaly::Repository.new(storage_name: storage, relative_path: relative_path)
  end
end

def start_gitaly
  build_dir = File.expand_path('../../../_build', __FILE__)
  gitlab_shell_dir = File.join(TMP_DIR, 'gitlab-shell')
  linguist_path = File.join(
    Bundler.rubygems.find_name('github-linguist').first.full_gem_path,
    "lib", "linguist", "languages.json"
  )

  FileUtils.mkdir_p([TMP_DIR, File.join(gitlab_shell_dir, 'hooks')])

  config_toml = <<~CONFIG
    socket_path = "#{SOCKET_PATH}"
    bin_dir = "#{build_dir}/bin"

    [gitlab-shell]
    dir = "#{gitlab_shell_dir}"

    [gitaly-ruby]
    dir = "#{build_dir}/assembly/ruby"
    linguist_languages_path = "#{linguist_path}"

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
end

start_gitaly
