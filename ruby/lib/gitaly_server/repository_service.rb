require 'licensee'

module GitalyServer
  class RepositoryService < Gitaly::RepositoryService::Service
    include Utils

    def create_repository(_request, call)
      repo_path = GitalyServer.repo_path(call)

      Gitlab::Git::Repository.create(repo_path)

      Gitaly::CreateRepositoryResponse.new
    end

    def fetch_source_branch(request, call)
      source_repository = Gitlab::Git::GitalyRemoteRepository.new(request.source_repository, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      result = repository.fetch_source_branch!(source_repository, request.source_branch, request.target_ref)

      Gitaly::FetchSourceBranchResponse.new(result: result)
    end

    def fetch_remote(request, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      if request.remote_params
        params = request.remote_params
        repository.add_remote(params.name, params.url)
      end

      gitlab_projects = Gitlab::Git::GitlabProjects.from_gitaly(request.repository, call)

      success = Gitlab::Git::SshAuth.from_gitaly(request).setup do |env|
        Gitlab::Git::HttpAuth.from_gitaly(request) do
          gitlab_projects.fetch_remote(
            request.remote,
            request.timeout,
            force: request.force,
            tags: !request.no_tags,
            env: env
          )
        end
      end

      raise GRPC::Unknown.new("Fetching remote #{request.remote} failed: #{gitlab_projects.output}") unless success

      Gitaly::FetchRemoteResponse.new
    end

    def write_config(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      repo.write_config(full_path: request.full_path)

      Gitaly::WriteConfigResponse.new
    rescue Rugged::Error => ex
      Gitaly::WriteConfigResponse.new(error: ex.message.b)
    end

    def set_config(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      request.entries.each do |entry|
        key = entry.key
        value = case entry.value
                when :value_str
                  entry.value_str
                when :value_int32
                  entry.value_int32
                when :value_bool
                  entry.value_bool
                else
                  raise GRPC::InvalidArgument, "unknown entry type: #{entry.value}"
                end

        repo.rugged.config[key] = value
      end

      Gitaly::SetConfigResponse.new
    end

    def find_license(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      short_name = begin
                     ::Licensee.license(repo.path).try(:key)
                   rescue Rugged::Error
                   end

      Gitaly::FindLicenseResponse.new(license_short_name: short_name || "")
    end

    # TODO: remove after 11.8 because of https://gitlab.com/gitlab-org/gitaly/merge_requests/1026
    def get_raw_changes(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

      changes = begin
                  repo
                    .raw_changes_between(request.from_revision, request.to_revision)
                    .map { |c| to_proto_raw_change(c) }
                rescue ::Gitlab::Git::Repository::GitError => e
                  raise GRPC::InvalidArgument.new(e.message)
                end

      Enumerator.new do |y|
        changes.each_slice(100) do |batch|
          y.yield Gitaly::GetRawChangesResponse.new(raw_changes: batch)
        end
      end
    end

    private

    OPERATION_MAP = {
      added:        Gitaly::GetRawChangesResponse::RawChange::Operation::ADDED,
      copied:       Gitaly::GetRawChangesResponse::RawChange::Operation::COPIED,
      deleted:      Gitaly::GetRawChangesResponse::RawChange::Operation::DELETED,
      modified:     Gitaly::GetRawChangesResponse::RawChange::Operation::MODIFIED,
      renamed:      Gitaly::GetRawChangesResponse::RawChange::Operation::RENAMED,
      type_changed: Gitaly::GetRawChangesResponse::RawChange::Operation::TYPE_CHANGED
    }.freeze

    def to_proto_raw_change(change)
      operation = OPERATION_MAP[change.operation] || Gitaly::GetRawChangesResponse::RawChange::Operation::UNKNOWN

      # Protobuf doesn't even try to call `to_s` or `to_i` where it might be needed.
      Gitaly::GetRawChangesResponse::RawChange.new(
        blob_id: change.blob_id.to_s,
        size: change.blob_size.to_i,
        new_path: change.new_path.to_s,
        old_path: change.old_path.to_s,
        operation: operation,
        old_mode: change.old_mode.to_i(8),
        new_mode: change.new_mode.to_i(8)
      )
    end
  end
end
