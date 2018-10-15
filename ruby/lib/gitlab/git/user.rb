module Gitlab
  module Git
    class User
      attr_reader :username, :name, :email, :gl_id

      def self.from_gitaly(gitaly_user)
        new(
          gitaly_user.gl_username,
          Gitlab::EncodingHelper.encode!(gitaly_user.name),
          Gitlab::EncodingHelper.encode!(gitaly_user.email),
          gitaly_user.gl_id
        )
      end

      def initialize(username, name, email, gl_id)
        @username = username
        @name = name
        @email = email
        @gl_id = gl_id
      end

      def ==(other)
        [username, name, email, gl_id] == [other.username, other.name, other.email, other.gl_id]
      end
    end
  end
end
