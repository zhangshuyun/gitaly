# frozen_string_literal: true

require 'grpc'
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
      @log_file.sync = true
      @default_tags = default_tags

      super()
    end

    def request_response(request: nil, call: nil, method: nil)
      log_request(method, call) { yield }
    end

    def server_streamer(request: nil, call: nil, method: nil)
      log_request(method, call) { yield }
    end

    def client_streamer(call: nil, method: nil)
      log_request(method, call) { yield }
    end

    def bidi_streamer(requests: nil, call: nil, method: nil)
      log_request(method, call) { yield }
    end

    private

    def log_request(method, call, &block)
      start = Time.now
      code = GRPC::Core::StatusCodes::OK

      block.call
    rescue => ex
      code = ex.is_a?(GRPC::BadStatus) ? ex.code : GRPC::Core::StatusCodes::UNAVAILABLE

      raise
    ensure
      message = @default_tags.merge(
        {
          'grpc.time_ms': ((Time.now - start) * 1000.0).truncate(3),
          'grpc.code': CODE_STRINGS[code] || code.to_s,
          'grpc.method': method_name(method) || '(Unknown)',
          'grpc.service': method.owner.service_name,
          pid: Process.pid,
          correlation_id: call.metadata['x-gitlab-correlation-id'],
          time: Time.now.utc.strftime('%Y-%m-%dT%H:%M:%S.%LZ')
        }
      )

      @log_file.puts(JSON.dump(message))
    end

    def method_name(method)
      result, = method.owner.rpc_descs.find do |k, _|
        GRPC::GenericService.underscore(k.to_s) == method.name.to_s
      end

      result
    end
  end
end
