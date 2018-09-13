require 'raven/base'

module GitalyServer
  module Sentry
    def self.enabled?
      ENV['SENTRY_DSN'].present?
    end
  end
end

class GitalyServer::Sentry::URLSanitizer < Raven::Processor
  include GitalyServer::Utils

  def process(data)
    sanitize_message(data)
    sanitize_fingerprint(data)
    sanitize_exceptions(data)
    sanitize_logentry(data)

    data
  end

  private

  def sanitize_logentry(data)
    logentry = data[:logentry]
    return unless logentry.is_a?(Hash)

    logentry[:message] = sanitize_url(logentry[:message])
  end

  def sanitize_fingerprint(data)
    fingerprint = data[:fingerprint]
    return unless fingerprint.is_a?(Array)

    fingerprint[-1] = sanitize_url(fingerprint.last)
  end

  def sanitize_exceptions(data)
    exception = data[:exception]
    return unless exception.is_a?(Hash)

    values = exception[:values]
    return unless values.is_a?(Array)

    values.each { |exception_data| exception_data[:value] = sanitize_url(exception_data[:value]) }
  end

  def sanitize_message(data)
    data[:message] = sanitize_url(data[:message])
  end
end

Raven.configure do |config|
  config.release = ENV['GITALY_VERSION'].presence
  config.sanitize_fields = %w[gitaly-servers authorization]
  config.processors += [GitalyServer::Sentry::URLSanitizer]
end
