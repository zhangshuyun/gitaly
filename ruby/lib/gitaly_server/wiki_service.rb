module GitalyServer
  class WikiService < Gitaly::WikiService::Service
    include Utils

    def wiki_delete_page(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      wiki = Gitlab::Git::Wiki.new(repo)
      page_path = set_utf8!(request.page_path)
      commit_details = commit_details_from_gitaly(request.commit_details)

      wiki.delete_page(page_path, commit_details)

      Gitaly::WikiDeletePageResponse.new
    end

    def wiki_write_page(call)
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

    def wiki_find_page(request, call)
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

      Enumerator.new do |y|
        y.yield Gitaly::WikiFindPageResponse.new(page: build_gitaly_wiki_page(page))

        io = StringIO.new(page.text_data)
        while chunk = io.read(Gitlab.config.git.write_buffer_size)
          gitaly_wiki_page = Gitaly::WikiPage.new(raw_data: chunk)

          y.yield Gitaly::WikiFindPageResponse.new(page: gitaly_wiki_page)
        end
      end
    end

    def wiki_get_all_pages(request, call)
      pages = get_wiki_pages(request, call)

      Enumerator.new do |y|
        pages.each do |page|
          y.yield Gitaly::WikiGetAllPagesResponse.new(page: build_gitaly_wiki_page(page))

          io = StringIO.new(page.text_data)
          while chunk = io.read(Gitlab.config.git.write_buffer_size)
            gitaly_wiki_page = Gitaly::WikiPage.new(raw_data: chunk)

            y.yield Gitaly::WikiGetAllPagesResponse.new(page: gitaly_wiki_page)
          end

          y.yield Gitaly::WikiGetAllPagesResponse.new(end_of_page: true)
        end
      end
    end

    def wiki_list_pages(request, call)
      pages = get_wiki_pages(request, call)

      Enumerator.new do |y|
        pages.each do |page|
          wiki_page = build_gitaly_wiki_page(page)

          y.yield Gitaly::WikiListPagesResponse.new(page: wiki_page)
        end
      end
    end

    def wiki_update_page(call)
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

    private

    def get_wiki_pages(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      wiki = Gitlab::Git::Wiki.new(repo)
      pages_limit = request.limit.zero? ? nil : request.limit

      wiki.pages(limit: pages_limit, sort: request.sort.to_s.downcase, direction_desc: request.direction_desc)
    end

    def build_gitaly_wiki_page(page = nil)
      return Gitaly::WikiPage.new unless page

      Gitaly::WikiPage.new(
        version: build_gitaly_page_version(page),
        format: page.format.to_s,
        title: page.title.b,
        url_path: page.url_path.to_s,
        path: page.path.b,
        name: page.name.b,
        historical: page.historical?
      )
    end

    def build_gitaly_page_version(page)
      Gitaly::WikiPageVersion.new(
        commit: gitaly_commit_from_rugged(page.version.commit.raw_commit),
        format: page.version.format.to_s
      )
    end
  end
end
