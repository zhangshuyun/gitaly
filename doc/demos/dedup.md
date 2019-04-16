# Git Object Deduplication demo

## Script

- Start with GDK with gitlab-ee master and gitaly master
- separate terminal: `gdk run db`
- `rm -f .gitlab-bundle .gitlab-yarn && make gitlab-update gitaly-update`
- `gdk run app`
- create PUBLIC group parent
- create PUBLIC project in parent group from import: https://gitlab.com/gitlab-org/gitlab-ce.git
- separate terminal: `bundle exec rails console`
- console:  `Feature.enable(:object_pools)`
- create group for fork
- fork into new group
- console: `ActiveRecord::Base.logger.level = Logger::INFO`
- console: `def sizes; Project.all.each { |p| printf("%4d %s\n", p.repository.raw.size, p.inspect) };nil; end`
- console: `def pool(id); PoolRepository.find(id); end`
- console: `def show_alt(p) ; system('cat', File.join('../repositories', p.repository.relative_path, 'objects/info/alternates')); end`
- try to break stuff: `PoolRepository.all.each(&:destroy)`. Do projects still work?


