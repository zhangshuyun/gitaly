require 'spec_helper'

describe Gitlab::Git::GitlabProjects do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(test_repo_read_only) }
  let(:repo_name) { 'gitlab-test.git' }
  let(:gl_projects) { build_gitlab_projects(DEFAULT_STORAGE_DIR, repo_name) }
  let(:hooks_path) { File.join(tmp_repo_path, 'hooks') }
  let(:tmp_repo_path) { TEST_REPO_PATH }
  let(:tmp_repos_path) { DEFAULT_STORAGE_DIR }

  if $VERBOSE
    let(:logger) { Logger.new(STDOUT) }
  else
    let(:logger) { double('logger').as_null_object }
  end

  def build_gitlab_projects(*args)
    described_class.new(
      *args,
      global_hooks_path: hooks_path,
      logger: logger
    )
  end

  def stub_spawn(*args, success: true)
    exitstatus = success ? 0 : nil

    expect(gl_projects)
      .to receive(:popen_with_timeout)
      .with(*args)
      .and_return(["output", exitstatus])
  end

  def stub_unlimited_spawn(*args, success: true)
    exitstatus = success ? 0 : nil

    expect(gl_projects)
      .to receive(:popen)
      .with(*args)
      .and_return(["output", exitstatus])
  end

  def stub_spawn_timeout(*args)
    expect(gl_projects).to receive(:popen_with_timeout).with(*args)
                                                       .and_raise(Timeout::Error)
  end

  describe '#initialize' do
    it { expect(gl_projects.shard_path).to eq(tmp_repos_path) }
    it { expect(gl_projects.repository_relative_path).to eq(repo_name) }
    it { expect(gl_projects.repository_absolute_path).to eq(File.join(tmp_repos_path, repo_name)) }
    it { expect(gl_projects.logger).to eq(logger) }
  end

  describe '#push_branches' do
    let(:remote_name) { 'remote-name' }
    let(:env) { { 'GIT_SSH_COMMAND' => 'foo-command bar' } }
    let(:force) { false }
    let(:branch_names) { 20.times.map { |i| "branch#{i}" } }
    let(:cmd1) do
      %W(#{Gitlab.config.git.bin_path} push --porcelain -- #{remote_name}) + branch_names[0, 10]
    end
    let(:cmd2) do
      %W(#{Gitlab.config.git.bin_path} push --porcelain -- #{remote_name}) + branch_names[10, 10]
    end

    subject { gl_projects.push_branches(remote_name, 600, force, branch_names, env: env) }

    it 'executes the command' do
      stub_spawn(cmd1, 600, tmp_repo_path, env, success: true)
      stub_spawn(cmd2, 600, tmp_repo_path, env, success: true)

      is_expected.to be_truthy
    end

    it 'fails' do
      stub_spawn(cmd1, 600, tmp_repo_path, env, success: true)
      stub_spawn(cmd2, 600, tmp_repo_path, env, success: false)

      is_expected.to be_falsy
    end

    context 'with --force' do
      let(:branch_names) { ['master'] }
      let(:cmd) { %W(#{Gitlab.config.git.bin_path} push --porcelain --force -- #{remote_name} #{branch_names[0]}) }
      let(:force) { true }

      it 'executes the command' do
        stub_spawn(cmd, 600, tmp_repo_path, env, success: true)

        is_expected.to be_truthy
      end
    end
  end

  describe '#fetch_remote' do
    let(:remote_name) { 'remote-name' }
    let(:branch_name) { 'master' }
    let(:force) { false }
    let(:tags) { true }
    let(:env) { { 'GIT_SSH_COMMAND' => 'foo-command bar' } }
    let(:prune) { true }
    let(:follow_redirects) { false }
    let(:args) { { force: force, tags: tags, env: env, prune: prune } }
    let(:cmd) { %W(#{Gitlab.config.git.bin_path} -c http.followRedirects=false fetch #{remote_name} --quiet --prune --tags) }

    subject { gl_projects.fetch_remote(remote_name, 600, args) }

    context 'with default args' do
      it 'executes the command' do
        stub_spawn(cmd, 600, tmp_repo_path, env, success: true)

        is_expected.to be_truthy
      end

      it 'returns false if the command fails' do
        stub_spawn(cmd, 600, tmp_repo_path, env, success: false)

        is_expected.to be_falsy
      end
    end

    context 'with --force' do
      let(:force) { true }
      let(:cmd) { %W(#{Gitlab.config.git.bin_path} -c http.followRedirects=false fetch #{remote_name} --quiet --prune --force --tags) }

      it 'executes the command with forced option' do
        stub_spawn(cmd, 600, tmp_repo_path, env, success: true)

        is_expected.to be_truthy
      end
    end

    context 'with --no-tags' do
      let(:tags) { false }
      let(:cmd) { %W(#{Gitlab.config.git.bin_path} -c http.followRedirects=false fetch #{remote_name} --quiet --prune --no-tags) }

      it 'executes the command' do
        stub_spawn(cmd, 600, tmp_repo_path, env, success: true)

        is_expected.to be_truthy
      end
    end

    context 'with no prune' do
      let(:prune) { false }
      let(:cmd) { %W(#{Gitlab.config.git.bin_path} -c http.followRedirects=false fetch #{remote_name} --quiet --tags) }

      it 'executes the command' do
        stub_spawn(cmd, 600, tmp_repo_path, env, success: true)

        is_expected.to be_truthy
      end
    end
  end

  describe '#delete_remote_branches' do
    let(:remote_name) { 'remote-name' }
    let(:branch_names) { 20.times.map { |i| "branch#{i}" } }
    let(:env) { { 'GIT_SSH_COMMAND' => 'foo-command bar' } }
    let(:cmd1) do
      %W(#{Gitlab.config.git.bin_path} push -- #{remote_name}) +
        branch_names[0, 10].map { |b| ':' + b }
    end
    let(:cmd2) do
      %W(#{Gitlab.config.git.bin_path} push -- #{remote_name}) +
        branch_names[10, 10].map { |b| ':' + b }
    end

    subject { gl_projects.delete_remote_branches(remote_name, branch_names, env: env) }

    it 'executes the command' do
      stub_unlimited_spawn(cmd1, tmp_repo_path, env, success: true)
      stub_unlimited_spawn(cmd2, tmp_repo_path, env, success: true)

      is_expected.to be_truthy
    end

    it 'fails' do
      stub_unlimited_spawn(cmd1, tmp_repo_path, env, success: true)
      stub_unlimited_spawn(cmd2, tmp_repo_path, env, success: false)

      is_expected.to be_falsy
    end
  end
end
