# frozen_string_literal: truer

require 'grpc'
require 'active_support/core_ext/string/inflections'
require 'json'

module GitalyServer
  class LoggingInterceptor < GRPC::ServerInterceptor
    CODE_STRINGS = {
      GRPC::Core::StatusCodes::OK => 'OK',
      GRPC::Core::StatusCodes::CANCELLED => 'Canceled',
      GRPC::Core::StatusCodes::UNKNOWN => 'Unknown',
      GRPC::Core::StatusCodes::INVALID_ARGUMENT => 'InvalidArgument',
      GRPC::Core::StatusCodes::DEADLINE_EXCEEDED => 'DeadlineExceeded',
      GRPC::Core::StatusCodes::NOT_FOUND => 'NotFound',
      GRPC::Core::StatusCodes::ALREADY_EXISTS => 'AlreadyExists',
      GRPC::Core::StatusCodes::PERMISSION_DENIED => 'PermissionDenied',
      GRPC::Core::StatusCodes::RESOURCE_EXHAUSTED => 'ResourceExhausted',
      GRPC::Core::StatusCodes::FAILED_PRECONDITION => 'FailedPrecondition',
      GRPC::Core::StatusCodes::ABORTED => 'Aborted',
      GRPC::Core::StatusCodes::OUT_OF_RANGE => 'OutOfRange',
      GRPC::Core::StatusCodes::UNIMPLEMENTED => 'Unimplemented',
      GRPC::Core::StatusCodes::INTERNAL => 'Internal',
      GRPC::Core::StatusCodes::UNAVAILABLE => 'Unavailable',
      GRPC::Core::StatusCodes::DATA_LOSS => 'DataLoss',
      GRPC::Core::StatusCodes::UNAUTHENTICATED => 'Unauthenticated'
    }.freeze

    def initialize(log_file, default_tags)
      @log_file = log_file
      @log_file.sync = true if @log_file.respond_to?(:sync=)
      @default_tags = default_tags

      super()
    end

    def request_response(request: nil, call: nil, method: nil)
      start = Time.now
      code = GRPC::Core::StatusCodes::OK

      yield
    rescue GRPC::BadStatus => e
      code = e.code
      raise
    ensure
      log_request(method, call, code, start)
    end

    def server_streamer(request: nil, call: nil, method: nil)
      start = Time.now
      code = GRPC::Core::StatusCodes::OK

      yield
    rescue GRPC::BadStatus => e
      code = e.code
      raise
    ensure
      log_request(method, call, code, start)
    end

    def client_streamer(call: nil, method: nil)
      start = Time.now
      code = GRPC::Core::StatusCodes::OK

      yield
    rescue GRPC::BadStatus => e
      code = e.code
      raise
    ensure
      log_request(method, call, code, start)
    end

    def bidi_streamer(requests: nil, call: nil, method: nil)
      yield
    rescue GRPC::BadStatus => e
      code = e.code
      raise
    ensure
      log_request(method, call, code, start)
    end

    private

    def log_request(method, call, code, start)
      message = @default_tags.merge(
        {
          duration: (Time.now - start).to_f,
          code: CODE_STRINGS[code] || code.to_s,
          method: method.name.to_s.camelize,
          service: method.owner.service_name,
          pid: Process.pid,
          correlation_id: call.metadata['x-gitlab-correlation-id'],
          time: Time.now.utc.strftime('%Y-%m-%dT%H:%M:%S.%LZ')
        }
      )
      log(message)
    end

    def log(message)
      @log_file.puts(JSON.dump(message))
    end
  end
end
