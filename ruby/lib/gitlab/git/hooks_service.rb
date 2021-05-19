module Gitlab
  module Git
    class HooksService
      attr_accessor :oldrev, :newrev, :ref

      def execute(pusher, repository, oldrev, newrev, ref, push_options:, transaction: nil)
        @repository  = repository
        @gl_id       = pusher.gl_id
        @gl_username = pusher.username
        @oldrev      = oldrev
        @newrev      = newrev
        @ref         = ref
        @push_options = push_options
        @transaction = transaction

        %w[pre-receive update reference-transaction].each do |hook_name|
          status, message = run_hook(hook_name)

          raise PreReceiveError, message unless status
        end

        yield(self).tap do
          status, message = run_hook('reference-transaction')
          Gitlab::GitLogger.error("reference-transaction committed hook: #{message}") unless status

          status, message = run_hook('post-receive')

          Gitlab::GitLogger.error("post-receive hook: #{message}") unless status
        end
      end

      private

      def run_hook(name)
        hook = Gitlab::Git::Hook.new(name, @repository)
        hook.trigger(@gl_id, @gl_username, oldrev, newrev, ref, push_options: @push_options, transaction: @transaction)
      end
    end
  end
end
