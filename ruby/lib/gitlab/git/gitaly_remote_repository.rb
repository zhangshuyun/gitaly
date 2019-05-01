module Gitlab
  module Git
    class GitalyRemoteRepository < RemoteRepository
      CLIENT_NAME = 'gitaly-ruby'.freeze
      PEM_REXP = /[-]+BEGIN CERTIFICATE[-]+.+?[-]+END CERTIFICATE[-]+/m

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

      def certs
        raise 'SSL_CERT_DIR and/or SSL_CERT_FILE environment variable must be set' unless ENV['SSL_CERT_DIR'] || ENV['SSL_CERT_FILE']

        return @certs if @certs

        files = []
        files += Dir["#{ENV['SSL_CERT_DIR']}/*"] if ENV['SSL_CERT_DIR']
        files += [ENV['SSL_CERT_FILE']] if ENV['SSL_CERT_FILE']
        files.sort!

        @certs = files.flat_map do |cert_file|
          File.read(cert_file).scan(PEM_REXP).map do |cert|
            begin
              OpenSSL::X509::Certificate.new(cert).to_pem
            rescue OpenSSL::OpenSSLError => e
              Rails.logger.error "Could not load certificate #{cert_file} #{e}"
              nil
            end
          end.compact
        end.uniq.join("\n")
      end

      def credentials
        if URI(gitaly_client.address(storage)).scheme == 'tls'
          GRPC::Core::ChannelCredentials.new certs
        else
          :this_channel_is_insecure
        end
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
        addr = addr.sub(%r{^tcp://|^tls://}, '') if %w[tcp tls].include? URI(addr).scheme
        addr
      end

      def token
        gitaly_client.token(storage).to_s
      end

      def request_kwargs
        @request_kwargs ||= begin
          metadata = {
            'authorization' => "Bearer #{auhtorization_token}",
            'client_name' => CLIENT_NAME
          }

          { metadata: metadata }
        end
      end

      def auhtorization_token
        issued_at = Time.now.to_i.to_s
        hmac = OpenSSL::HMAC.hexdigest(OpenSSL::Digest::SHA256.new, token, issued_at)

        "v2.#{hmac}.#{issued_at}"
      end
    end
  end
end
