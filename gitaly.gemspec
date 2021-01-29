# coding: utf-8
prefix = 'ruby/proto'
$LOAD_PATH.unshift(File.expand_path(File.join(prefix), __dir__))
require 'gitaly/version'

Gem::Specification.new do |spec|
  spec.name          = "gitaly"
  spec.version       = Gitaly::VERSION
  spec.authors       = ["Jacob Vosmaer"]
  spec.email         = ["jacob@gitlab.com"]

  spec.summary       = %q{Auto-generated gRPC client for gitaly}
  spec.description   = %q{Auto-generated gRPC client for gitaly.}
  spec.homepage      = "https://gitlab.com/gitlab-org/gitaly"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z #{prefix}`.split("\x0").reject { |f| f.match(%r{^#{prefix}/(test|spec|features)/}) }
  spec.require_paths = [prefix]

  spec.add_dependency "grpc", "~> 1.0"
  # This is locked to ensure CE/EE use the same version to ensure
  # license key mappings match between gitaly-ruby and GitLab Rails.
  spec.add_dependency "licensee", "= 9.14.1"
end
