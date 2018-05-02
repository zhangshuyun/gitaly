module GitalyServer
  class WikiService < Gitaly::WikiService::Service
    include Utils

    def wiki_delete_page(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        wiki = Gitlab::Git::Wiki.new(repo)
        page_path = set_utf8!(request.page_path)
        commit_details = commit_details_from_gitaly(request.commit_details)

        wiki.delete_page(page_path, commit_details)

        Gitaly::WikiDeletePageResponse.new
      end
    end

    def wiki_write_page(call)
      bridge_exceptions do
        begin
          repo = name = format = commit_details = nil
          content = ""

          call.each_remote_read.with_index do |request, index|
            if index.zero?
              repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
              name = set_utf8!(request.name)
              format = request.format
              commit_details = request.commit_details
            end

            content << request.content
          end

          wiki = Gitlab::Git::Wiki.new(repo)
          commit_details = commit_details_from_gitaly(commit_details)

          wiki.write_page(name, format.to_sym, content, commit_details)

          Gitaly::WikiWritePageResponse.new
        rescue Gitlab::Git::Wiki::DuplicatePageError => e
          Gitaly::WikiWritePageResponse.new(duplicate_error: e.message.b)
        end
      end
    end

    def wiki_find_page(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        wiki = Gitlab::Git::Wiki.new(repo)

        page = wiki.page(
          title: set_utf8!(request.title),
          version: request.revision.presence,
          dir: set_utf8!(request.directory)
        )

        unless page
          return Enumerator.new do |y|
            y.yield Gitaly::WikiFindPageResponse.new
          end
        end

        version = Gitaly::WikiPageVersion.new(
          commit: gitaly_commit_from_rugged(page.version.commit.raw_commit),
          format: page.version.format.to_s
        )
        gitaly_wiki_page = Gitaly::WikiPage.new(
          version: version,
          format: page.format.to_s,
          title: page.title.b,
          url_path: page.url_path.to_s,
          path: page.path.b,
          name: page.name.b,
          historical: page.historical?
        )

        Enumerator.new do |y|
          io = StringIO.new(page.text_data)
          while chunk = io.read(Gitlab.config.git.write_buffer_size)
            gitaly_wiki_page.raw_data = chunk

            y.yield Gitaly::WikiFindPageResponse.new(page: gitaly_wiki_page)

            gitaly_wiki_page = Gitaly::WikiPage.new
          end
        end
      end
    end

    def wiki_get_all_pages(request, call)
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        wiki = Gitlab::Git::Wiki.new(repo)

        Enumerator.new do |y|
          wiki.pages.each do |page|
            version = Gitaly::WikiPageVersion.new(
              commit: gitaly_commit_from_rugged(page.version.commit.raw_commit),
              format: page.version.format.to_s
            )
            gitaly_wiki_page = Gitaly::WikiPage.new(
              version: version,
              format: page.format.to_s,
              title: page.title.b,
              url_path: page.url_path.to_s,
              path: page.path.b,
              name: page.name.b,
              historical: page.historical?
            )

            io = StringIO.new(page.text_data)
            while chunk = io.read(Gitlab.config.git.write_buffer_size)
              gitaly_wiki_page.raw_data = Gitlab::EncodingHelper.encode!(chunk)

              y.yield Gitaly::WikiGetAllPagesResponse.new(page: gitaly_wiki_page)

              gitaly_wiki_page = Gitaly::WikiPage.new
            end

            y.yield Gitaly::WikiGetAllPagesResponse.new(end_of_page: true)
          end
        end
    rescue 
          warn caller[0..25].join(' ')
    end

    def wiki_find_file(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        wiki = Gitlab::Git::Wiki.new(repo)

        file = wiki.file(set_utf8!(request.name), request.revision.presence)

        unless file
          return Enumerator.new do |y|
            y.yield Gitaly::WikiFindFileResponse.new
          end
        end

        response = Gitaly::WikiFindFileResponse.new(
          name: file.name.b,
          mime_type: file.mime_type,
          path: file.path.b
        )

        Enumerator.new do |y|
          io = StringIO.new(file.raw_data)
          while chunk = io.read(Gitlab.config.git.write_buffer_size)
            response.raw_data = chunk

            y.yield response

            response = Gitaly::WikiFindFileResponse.new
          end
        end
      end
    end

    def wiki_get_page_versions(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        wiki = Gollum::Wiki.new(repo.path)
        path = set_utf8!(request.page_path)

        page = wiki.paged(Gollum::Page.canonicalize_filename(path), File.split(path).first)

        unless page
          return Enumerator.new do |y|
            y.yield Gitaly::WikiGetPageVersionsResponse.new(versions: [])
          end
        end

        Enumerator.new do |y|
          page.versions(per_page: request.per_page, page: request.page).each_slice(20) do |slice|
            versions =
              slice.map do |commit|
                gollum_page = wiki.page(page.title, commit.id)
                obj = repo.rugged.rev_parse(commit.id)

                Gitaly::WikiPageVersion.new(
                  commit: gitaly_commit_from_rugged(obj),
                  format: gollum_page&.format.to_s
                )
              end

            y.yield Gitaly::WikiGetPageVersionsResponse.new(versions: versions)
          end
        end
      end
    end

    def wiki_update_page(call)
      bridge_exceptions do
        repo = wiki = title = format = page_path = commit_details = nil
        content = ""

        call.each_remote_read.with_index do |request, index|
          if index.zero?
            repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
            wiki = Gitlab::Git::Wiki.new(repo)
            title = set_utf8!(request.title)
            page_path = set_utf8!(request.page_path)
            format = request.format

            commit_details = commit_details_from_gitaly(request.commit_details)
          end

          content << request.content
        end

        wiki.update_page(page_path, title, format.to_sym, content, commit_details)

        Gitaly::WikiUpdatePageResponse.new
      end
    end

    def wiki_get_formatted_data(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        wiki = Gitlab::Git::Wiki.new(repo)

        page = wiki.page(
          title: set_utf8!(request.title),
          version: request.revision.presence,
          dir: set_utf8!(request.directory).presence
        )

        raise GRPC::NotFound unless page

        Enumerator.new do |y|
          io = StringIO.new(page.formatted_data)
          while chunk = io.read(Gitlab.config.git.write_buffer_size)
            y.yield Gitaly::WikiGetFormattedDataResponse.new(data: chunk)
          end
        end
      end
    end
  end
end
