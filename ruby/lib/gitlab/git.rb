# External dependencies of Gitlab::Git
require 'rugged'
require 'linguist/blob_helper'
require 'securerandom'

# Ruby on Rails mix-ins that GitLab::Git code relies on
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/numeric/bytes'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/integer/time'
require 'active_support/core_ext/module/delegation'
require 'active_support/core_ext/enumerable'

require_relative 'git_logger.rb'
require_relative 'rails_logger.rb'
require_relative 'gollum.rb'
require_relative 'config.rb'

dir = __dir__

# Some later requires are order-sensitive. Manually require whatever we need.
require_relative "#{dir}/encoding_helper.rb"
require_relative "#{dir}/utils/strong_memoize.rb"
require_relative "#{dir}/ref_matcher.rb"
require_relative "#{dir}/git/remote_repository.rb"
require_relative "#{dir}/git/popen.rb"
require_relative "#{dir}/git/repository_mirroring.rb"

# Require all .rb files we can find in the gitlab lib directory
Dir["#{dir}/**/*.rb"].sort.each do |ruby_file|
  require File.expand_path(ruby_file)
end

class String
  # Because we are not rendering HTML, this is a no-op in gitaly-ruby.
  def html_safe
    self
  end
end

module Gitlab
  module Git
    # The ID of empty tree.
    # See http://stackoverflow.com/a/40884093/1856239 and
    # https://github.com/git/git/blob/3ad8b5bf26362ac67c9020bf8c30eee54a84f56d/cache.h#L1011-L1012
    EMPTY_TREE_ID = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'.freeze
    BLANK_SHA = ('0' * 40).freeze
    TAG_REF_PREFIX = "refs/tags/".freeze
    BRANCH_REF_PREFIX = "refs/heads/".freeze

    BaseError = Class.new(StandardError)
    CommandError = Class.new(BaseError)
    CommitError = Class.new(BaseError)
    OSError = Class.new(BaseError)
    UnknownRef = Class.new(BaseError)
    PreReceiveError = Class.new(BaseError)
    PatchError = Class.new(BaseError)

    class << self
      include Gitlab::EncodingHelper

      def ref_name(ref)
        encode!(ref).sub(%r{\Arefs/(tags|heads|remotes)/}, '')
      end

      def branch_name(ref)
        ref = ref.to_s
        self.ref_name(ref) if self.branch_ref?(ref)
      end

      def committer_hash(email:, name:, timestamp: nil)
        return if email.nil? || name.nil?

        # Git strips newlines and angle brackets silently.
        # libgit2/Rugged doesn't, but aborts if angle brackets are present.
        # See upstream issue https://github.com/libgit2/libgit2/issues/5342
        email = email.delete("\n<>")
        name = name.delete("\n<>")
        time = timestamp ? Time.at(timestamp.seconds, timestamp.nanos, :nsec, in: "+00:00") : Time.now

        {
          email: email,
          name: name,
          time: time
        }
      end

      def tag_name(ref)
        ref = ref.to_s
        self.ref_name(ref) if self.tag_ref?(ref)
      end

      def tag_ref?(ref)
        ref.start_with?(TAG_REF_PREFIX)
      end

      def branch_ref?(ref)
        ref.start_with?(BRANCH_REF_PREFIX)
      end

      def blank_ref?(ref)
        ref == BLANK_SHA
      end

      def version
        Gitlab::Git::Version.git_version
      end

      def diff_line_code(file_path, new_line_position, old_line_position)
        "#{Digest::SHA1.hexdigest(file_path)}_#{old_line_position}_#{new_line_position}"
      end

      def shas_eql?(sha1, sha2)
        return false if sha1.nil? || sha2.nil?
        return false unless sha1.class == sha2.class

        # If either of the shas is below the minimum length, we cannot be sure
        # that they actually refer to the same commit because of hash collision.
        length = [sha1.length, sha2.length].min
        return false if length < Gitlab::Git::Commit::MIN_SHA_LENGTH

        sha1[0, length] == sha2[0, length]
      end
    end
  end
end

module Gitlab
  module GlId
    def self.gl_id(user)
      user.gl_id
    end

    def self.gl_id_from_id_value(id)
      "user-#{id}"
    end
  end
end
