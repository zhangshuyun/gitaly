require 'licensee'

module GitalyServer
  class RepositoryService < Gitaly::RepositoryService::Service
    include Utils

    def fetch_source_branch(request, call)
      source_repository = Gitlab::Git::GitalyRemoteRepository.new(request.source_repository, call)
      repository = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      result = repository.fetch_source_branch!(source_repository, request.source_branch, request.target_ref)

      Gitaly::FetchSourceBranchResponse.new(result: result)
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
  end
end
