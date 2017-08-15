module Gitlab
  module GitalyClient
    class << self
      # In case we hit a method that tries to do a Gitaly RPC, prevent this.
      # We also don't want to instrument the block.
      def migrate(*args)
        yield false # 'false' means 'don't use gitaly for this block'
      end
    end
  end
end
