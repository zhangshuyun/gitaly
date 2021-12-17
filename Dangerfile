# frozen_string_literal: true

require 'gitlab-dangerfiles'

Gitlab::Dangerfiles.for_project(self) do |dangerfiles|
  dangerfiles.import_defaults

  danger.import_plugin('danger/plugins/*.rb')

  Dir.each_child('danger/rules') do |rule|
    danger.import_dangerfile(path: "danger/rules/#{rule}")
  end

  anything_to_post = status_report.values.any?(&:any?)

  if helper.ci? && anything_to_post
    markdown("**If needed, you can retry the [`danger-review` job](#{ENV['CI_JOB_URL']}) that generated this comment.**")
  end
end

# vim: ft=ruby
