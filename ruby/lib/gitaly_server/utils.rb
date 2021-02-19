module GitalyServer
  module Utils
    # See internal/logsanitizer/url.go for credits and explanation.
    URL_HOST_PATTERN = %r{([a-z][a-z0-9+\-.]*://)?([a-z0-9\-._~%!$&'()*+,;=:]+@)([a-z0-9\-._~%]+|\[[a-z0-9\-._~%!$&'()*+,;=:]+\])}i.freeze

    def gitaly_commit_from_rugged(rugged_commit)
      message_split = rugged_commit.message.b.split("\n", 2)
      gitaly_commit = Gitaly::GitCommit.new(
        id: rugged_commit.oid,
        subject: message_split[0] ? message_split[0].chomp : "",
        body: rugged_commit.message.b,
        parent_ids: rugged_commit.parent_ids,
        author: gitaly_commit_author_from_rugged(rugged_commit.author),
        committer: gitaly_commit_author_from_rugged(rugged_commit.committer),
        body_size: rugged_commit.message.bytesize,
        tree_id: rugged_commit.tree.oid,
        trailers: gitaly_trailers_from_rugged(rugged_commit)
      )
      truncate_gitaly_commit_body!(gitaly_commit) if gitaly_commit.body.bytesize > Gitlab.config.git.max_commit_or_tag_message_size

      gitaly_commit
    end

    def gitaly_commit_author_from_rugged(rugged_author)
      Gitaly::CommitAuthor.new(
        name: rugged_author[:name].b,
        email: rugged_author[:email].b,
        date: Google::Protobuf::Timestamp.new(seconds: rugged_author[:time].to_i),
        timezone: rugged_author[:time].strftime("%z")
      )
    end

    def commit_details_from_gitaly(gitaly_commit_details)
      Gitlab::Git::Wiki::CommitDetails.new(
        gitaly_commit_details.user_id,
        gitaly_commit_details.user_name,
        gitaly_commit_details.name,
        gitaly_commit_details.email,
        gitaly_commit_details.message
      )
    end

    def gitaly_tag_from_gitlab_tag(gitlab_tag, commit = nil)
      tag_message = gitlab_tag.message.to_s
      tag = Gitaly::Tag.new(
        name: gitlab_tag.name.b,
        id: gitlab_tag.target,
        message: tag_message.b,
        target_commit: commit,
        message_size: tag_message.bytesize
      )

      truncate_gitaly_tag_message!(tag) if tag.message.bytesize > Gitlab.config.git.max_commit_or_tag_message_size

      tag
    end

    def gitaly_trailers_from_rugged(rugged_commit)
      rugged_commit.trailers.map do |(key, value)|
        Gitaly::CommitTrailer.new(key: key.b, value: value.b)
      end
    end

    def truncate_gitaly_commit_body!(gitaly_commit)
      gitaly_commit.body = gitaly_commit.body[0, Gitlab.config.git.max_commit_or_tag_message_size]
    end

    def truncate_gitaly_tag_message!(gitaly_tag)
      gitaly_tag.message = gitaly_tag.message[0, Gitlab.config.git.max_commit_or_tag_message_size]
    end

    def set_utf8!(str)
      raise ArgumentError unless str.respond_to?(:force_encoding)
      return str if str.encoding == Encoding::UTF_8 && str.valid_encoding?

      str = str.dup if str.respond_to?(:frozen?) && str.frozen?

      str.force_encoding('UTF-8')
      str.valid_encoding? ? str : raise(ArgumentError, "string is not valid UTF-8: #{str.inspect}")
    end

    def sanitize_url(str)
      str.gsub(URL_HOST_PATTERN, '\1[FILTERED]@\3\4')
    end

    def parse_refmaps(refmaps)
      return unless refmaps.present?

      parsed_refmaps = refmaps.select(&:present?).map do |refmap|
        refmap_spec = refmap.to_sym

        if Gitlab::Git::RepositoryMirroring::REFMAPS.key?(refmap_spec)
          refmap_spec
        else
          refmap
        end
      end

      parsed_refmaps.presence
    end
  end
end
