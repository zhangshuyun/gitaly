module Gitlab
  module Git
    class Blob
      include Linguist::BlobHelper
      include Gitlab::EncodingHelper

      # This number is the maximum amount of data that we want to display to
      # the user. We load as much as we can for encoding detection
      # (Linguist) and LFS pointer parsing.
      MAX_DATA_DISPLAY_SIZE = 10.megabytes

      # These limits are used as a heuristic to ignore files which can't be LFS
      # pointers. The format of these is described in
      # https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer
      LFS_POINTER_MIN_SIZE = 120.bytes
      LFS_POINTER_MAX_SIZE = 200.bytes

      attr_accessor :size, :mode, :id, :commit_id, :loaded_size, :binary
      attr_writer :data, :name, :path

      class << self
        def find(repository, sha, path, limit: MAX_DATA_DISPLAY_SIZE)
          return unless path

          # Strip any leading / characters from the path
          path = path.sub(%r{\A/*}, '')

          rugged_commit = repository.lookup(sha)
          root_tree = rugged_commit.tree

          blob_entry = find_entry_by_path(repository, root_tree.oid, *path.split('/'))

          return nil unless blob_entry

          if blob_entry[:type] == :commit
            submodule_blob(blob_entry, path, sha)
          else
            blob = repository.lookup(blob_entry[:oid])

            if blob
              new(
                id: blob.oid,
                name: blob_entry[:name],
                size: blob.size,
                # Rugged::Blob#content is expensive; don't call it if we don't have to.
                data: limit.zero? ? '' : blob.content(limit),
                mode: blob_entry[:filemode].to_s(8),
                path: path,
                commit_id: sha,
                binary: blob.binary?
              )
            end
          end
        rescue Rugged::ReferenceError
          nil
        end

        # Find LFS blobs given an array of sha ids
        # Returns array of Gitlab::Git::Blob
        # Does not guarantee blob data will be set
        def batch_lfs_pointers(repository, blob_ids, limit: nil)
          selector = blob_ids.lazy
                             .select { |sha| possible_lfs_blob?(repository, sha) }
                             .map { |sha| rugged_raw(repository, sha, limit: LFS_POINTER_MAX_SIZE) }
                             .select(&:lfs_pointer?)

          return selector.first(limit) if limit

          selector.force
        end

        def binary?(data)
          EncodingHelper.detect_libgit2_binary?(data)
        end

        def size_could_be_lfs?(size)
          size.between?(LFS_POINTER_MIN_SIZE, LFS_POINTER_MAX_SIZE)
        end

        private

        # Recursive search of blob id by path
        #
        # Ex.
        #   blog/            # oid: 1a
        #     app/           # oid: 2a
        #       models/      # oid: 3a
        #       file.rb      # oid: 4a
        #
        #
        # Blob.find_entry_by_path(repo, '1a', 'blog', 'app', 'file.rb') # => '4a'
        #
        def find_entry_by_path(repository, root_id, *path_parts)
          root_tree = repository.lookup(root_id)

          entry = root_tree.find do |entry|
            entry[:name] == path_parts[0]
          end

          return nil unless entry

          if path_parts.size > 1
            return nil unless entry[:type] == :tree

            path_parts.shift
            find_entry_by_path(repository, entry[:oid], *path_parts)
          else
            [:blob, :commit].include?(entry[:type]) ? entry : nil
          end
        end

        def submodule_blob(blob_entry, path, sha)
          new(
            id: blob_entry[:oid],
            name: blob_entry[:name],
            size: 0,
            data: '',
            path: path,
            commit_id: sha
          )
        end

        def rugged_raw(repository, sha, limit:)
          blob = repository.lookup(sha)

          return unless blob.is_a?(Rugged::Blob)

          new(
            id: blob.oid,
            size: blob.size,
            data: blob.content(limit),
            binary: blob.binary?
          )
        end

        # Efficient lookup to determine if object size
        # and type make it a possible LFS blob without loading
        # blob content into memory with repository.lookup(sha)
        def possible_lfs_blob?(repository, sha)
          object_header = repository.rugged.read_header(sha)

          object_header[:type] == :blob &&
            size_could_be_lfs?(object_header[:len])
        end
      end

      def initialize(options)
        %w(id name path size data mode commit_id binary).each do |key|
          self.__send__("#{key}=", options[key.to_sym])
        end

        # Retain the actual size before it is encoded
        @loaded_size = @data.bytesize if @data
        @loaded_all_data = @loaded_size == size
      end

      def binary?
        @binary.nil? ? super : @binary == true
      end

      def data
        encode! @data
      end

      def name
        encode! @name
      end

      def path
        encode! @path
      end

      def truncated?
        size && (size > loaded_size)
      end

      # Valid LFS object pointer is a text file consisting of
      # version
      # oid
      # size
      # see https://github.com/github/git-lfs/blob/v1.1.0/docs/spec.md#the-pointer
      def lfs_pointer?
        self.class.size_could_be_lfs?(size) && has_lfs_version_key? && lfs_oid.present? && lfs_size.present?
      end

      def lfs_oid
        if has_lfs_version_key?
          oid = data.match(/(?<=sha256:)([0-9a-f]{64})/)
          return oid[1] if oid
        end

        nil
      end

      def lfs_size
        if has_lfs_version_key?
          size = data.match(/(?<=size )([0-9]+)/)
          return size[1].to_i if size
        end

        nil
      end

      def external_storage
        return unless lfs_pointer?

        :lfs
      end

      alias_method :external_size, :lfs_size

      private

      def has_lfs_version_key?
        !empty? && text? && data.start_with?("version https://git-lfs.github.com/spec")
      end
    end
  end
end
