module Git
  module Gitlab
    class HttpAuth
      def self.from_gitaly(request)
        repo = request.repository
        params = request.remote_params
        # validate params, don't set config if bad, or raise error?

        key = "http.#{params.url}.extraHeader"
        repo.rugged.config[key] = params.httpAuth
        
        begin
          yield # yield back to fetch_remote RPC handler
        ensure
          repo.rugged.config.delete(key)
        end
      end
    end
  end
end
