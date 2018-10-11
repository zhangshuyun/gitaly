require 'fileutils'
require 'securerandom'
require 'gitaly'
require 'rugged'

DEFAULT_STORAGE_DIR = File.expand_path('../../tmp/repositories', __FILE__)
DEFAULT_STORAGE_NAME = 'default'.freeze
TEST_REPO_PATH = File.join(DEFAULT_STORAGE_DIR, 'gitlab-test.git')
TEST_REPO_ORIGIN = '../internal/testhelper/testdata/data/gitlab-test.git'.freeze
GIT_TEST_REPO_PATH = File.join(DEFAULT_STORAGE_DIR, 'gitlab-git-test.git')
GIT_TEST_REPO_ORIGIN = '../internal/testhelper/testdata/data/gitlab-git-test.git'.freeze

module TestRepo
  def self.prepare_test_repository
    FileUtils.rm_rf(Dir["#{DEFAULT_STORAGE_DIR}/mutable-*"])

    FileUtils.mkdir_p(DEFAULT_STORAGE_DIR)

    {
      TEST_REPO_ORIGIN => TEST_REPO_PATH,
      GIT_TEST_REPO_ORIGIN => GIT_TEST_REPO_PATH
    }.each do |origin, path|
      next if File.directory?(path)

      clone_new_repo!(origin, path)
    end
  end

  def git_test_repo_read_only
    Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: File.basename(GIT_TEST_REPO_PATH))
  end

  def test_repo_read_only
    Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: File.basename(TEST_REPO_PATH))
  end

  def new_mutable_test_repo
    relative_path = random_repository_relative_path(:mutable)
    TestRepo.clone_new_repo!(TEST_REPO_ORIGIN, File.join(DEFAULT_STORAGE_DIR, relative_path))
    Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: relative_path)
  end

  def new_broken_test_repo
    relative_path = random_repository_relative_path(:broken)
    repo_path = File.join(DEFAULT_STORAGE_DIR, relative_path)
    TestRepo.clone_new_repo!(TEST_REPO_ORIGIN, repo_path)

    refs_path = File.join(repo_path, 'refs')
    FileUtils.rm_r(refs_path)

    Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: relative_path)
  end

  def new_empty_test_repo
    relative_path = random_repository_relative_path(:mutable)
    TestRepo.init_new_repo!(File.join(DEFAULT_STORAGE_DIR, relative_path))
    Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: relative_path)
  end

  def rugged_from_gitaly(gitaly_repo)
    Rugged::Repository.new(repo_path_from_gitaly(gitaly_repo))
  end

  def repo_path_from_gitaly(gitaly_repo)
    storage_name = gitaly_repo.storage_name
    raise "this helper does not know storage #{storage_name.inspect}" unless storage_name == DEFAULT_STORAGE_NAME
    File.join(DEFAULT_STORAGE_DIR, gitaly_repo.relative_path)
  end

  def gitlab_git_from_gitaly(gitaly_repo)
    Gitlab::Git::Repository.new(
      gitaly_repo,
      repo_path_from_gitaly(gitaly_repo),
      '',
      nil,
      ''
    )
  end

  def repository_from_relative_path(relative_path)
    gitlab_git_from_gitaly(
      Gitaly::Repository.new(storage_name: DEFAULT_STORAGE_NAME, relative_path: relative_path)
    )
  end

  def self.clone_new_repo!(origin, destination)
    return if system(*%W[git clone --quiet --bare #{origin} #{destination}])
    abort "Failed to clone test repo. Try running 'make prepare-tests' and try again."
  end

  def self.init_new_repo!(destination)
    return if system(*%W[git init --quiet --bare #{destination}])

    abort "Failed to init test repo."
  end

  private

  def random_repository_relative_path(prefix)
    "#{prefix}-#{SecureRandom.hex(6)}.git"
  end
end

TestRepo.prepare_test_repository
