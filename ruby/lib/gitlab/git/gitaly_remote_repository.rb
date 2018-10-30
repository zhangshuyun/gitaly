module Gitlab
  module Git
    class GitalyRemoteRepository < RemoteRepository
      CLIENT_NAME = 'gitaly-ruby'.freeze

      attr_reader :gitaly_client

      def initialize(gitaly_repository, call)
        @gitaly_repository = gitaly_repository
        @storage = gitaly_repository.storage_name
        @gitaly_client = GitalyServer.client(call)
      end

      def path
        raise 'gitaly-ruby cannot access remote repositories by path'
      end

      def empty?
        !exists? || !has_visible_content?
      end

      def branch_exists?(branch_name)
        request = Gitaly::RefExistsRequest.new(repository: @gitaly_repository, ref: "refs/heads/#{branch_name}".b)
        stub = Gitaly::RefService::Stub.new(address, credentials)
        stub.ref_exists(request, request_kwargs).value
      end

      def commit_id(revision)
        request = Gitaly::FindCommitRequest.new(repository: @gitaly_repository, revision: revision.b)
        stub = Gitaly::CommitService::Stub.new(address, credentials)
        stub.find_commit(request, request_kwargs)&.commit&.id.presence
      end

      private

      def exists?
        request = Gitaly::RepositoryExistsRequest.new(repository: @gitaly_repository)
        stub = Gitaly::RepositoryService::Stub.new(address, credentials)
        stub.repository_exists(request, request_kwargs).exists
      end

      def has_visible_content?
        request = Gitaly::HasLocalBranchesRequest.new(repository: @gitaly_repository)
        stub = Gitaly::RepositoryService::Stub.new(address, credentials)
        stub.has_local_branches(request, request_kwargs).value
      end

      def address
        addr = gitaly_client.address(storage)
        addr = addr.sub(%r{^tcp://}, '') if URI(addr).scheme == 'tcp'
        addr
      end

      def credentials
        :this_channel_is_insecure
      end

      def token
        @token ||= gitaly_client.token(storage).to_s
      end

      def request_kwargs
        @request_kwargs ||= begin
          metadata = {
            'authorization' => "Bearer #{authorization_token}",
            'client_name' => CLIENT_NAME
          }

          { metadata: metadata }
        end
      end

      def authorization_token
        # TODO: Remove support for direct secret handling (i.e. v1 auth scheme)
        # https://gitlab.com/gitlab-org/gitaly/issues/1388
        return token if token.split('.')[0] == 'v2'

        issued_at = Time.now.to_i.to_s
        hmac = OpenSSL::HMAC.hexdigest(OpenSSL::Digest::SHA256.new, token, issued_at)

        "v2.#{hmac}.#{issued_at}"
      end
    end
  end
end
