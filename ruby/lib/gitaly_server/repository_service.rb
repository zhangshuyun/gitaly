require 'licensee'

module GitalyServer
  class RepositoryService < Gitaly::RepositoryService::Service
    include Utils

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
