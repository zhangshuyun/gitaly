# frozen_string_literal: true

require 'gitlab-dangerfiles'

GITALY_TEAM = %w[
  8bitlife
  avar
  chriscool
  pks-t
  proglottis
  samihiltunen
  toon
]

gitlab_dangerfiles = Gitlab::Dangerfiles::Engine.new(self)
gitlab_dangerfiles.import_plugins

danger.import_plugin('danger/plugins/*.rb')

gitlab_dangerfiles.import_dangerfiles

Dir.each_child('danger/rules') do |rule|
  danger.import_dangerfile(path: "danger/rules/#{rule}")
end

anything_to_post = status_report.values.any?(&:any?)

if helper.ci? && anything_to_post
  markdown("**If needed, you can retry the [`danger-review` job](#{ENV['CI_JOB_URL']}) that generated this comment.**")
end

# vim: ft=ruby
