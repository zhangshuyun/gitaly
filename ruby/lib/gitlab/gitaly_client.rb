module Gitlab
  module GitalyClient
    module MigrationStatus
      OPT_IN = :fake_constant
      OPT_OUT = :fake_constant
      DISABLED = :fake_constant
    end

    class << self
      # In case we hit a method that tries to do a Gitaly RPC, we want to
      # prevent this most of the time.
      def migrate(*args)
        whitelist = [:fetch_ref]
        yield whitelist.include?(args.first)
      end
    end
  end
end
