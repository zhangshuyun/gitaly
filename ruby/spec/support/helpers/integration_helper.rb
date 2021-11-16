require 'socket'
require 'spec_helper'

SOCKET_PATH = 'gitaly.socket'.freeze

module GitalyConfig
  def self.set_dynamic_ports
    tcp_sock = Socket.new(:INET, :STREAM)
    tls_sock = Socket.new(:INET, :STREAM)
    tcp_sock.bind(Addrinfo.tcp('127.0.0.1', 0))
    tls_sock.bind(Addrinfo.tcp('127.0.0.1', 0))

    @dynamic_tcp_port = tcp_sock.local_address.ip_port
    @dynamic_tls_port = tls_sock.local_address.ip_port
  ensure
    tcp_sock.close
    tls_sock.close
  end

  def self.dynamic_port(type)
    set_dynamic_ports unless @dynamic_tcp_port && @dynamic_tls_port

    case type
    when 'tcp'
      @dynamic_tcp_port
    when 'tls'
      @dynamic_tls_port
    end
  end
end

module IntegrationClient
  def gitaly_stub(service, type = 'unix')
    klass = Gitaly.const_get(service).const_get(:Stub)
    addr = case type
           when 'unix'
             "unix:#{File.join(TMP_DIR, SOCKET_PATH)}"
           when 'tcp', 'tls'
             "#{type}://localhost:#{GitalyConfig.dynamic_port(type)}"
           end
    klass.new(addr, creds)
  end

  def creds
    :this_channel_is_insecure
  end

  def gitaly_repo(storage, relative_path)
    Gitaly::Repository.new(storage_name: storage, relative_path: relative_path)
  end

  def get_client(addr)
    servers = Base64.strict_encode64({
      default: {
        address: addr,
        token: 'the-secret-token'
      }
    }.to_json)

    call = double(metadata: { 'gitaly-servers' => servers })
    Gitlab::Git::GitalyRemoteRepository.new(repository.gitaly_repository, call)
  end
end

def start_gitaly
  build_dir = File.expand_path(File.join(GITALY_RUBY_DIR, '../_build'))
  cert_path = File.join(File.dirname(__FILE__), "/certs")

  FileUtils.mkdir_p([TMP_DIR, File.join(GITLAB_SHELL_DIR, 'hooks')])
  File.write(File.join(GITLAB_SHELL_DIR, '.gitlab_shell_secret'), 'test_gitlab_shell_token')

  config_toml = <<~CONFIG
    socket_path = "#{SOCKET_PATH}"
    listen_addr = "localhost:#{GitalyConfig.dynamic_port('tcp')}"
    tls_listen_addr = "localhost:#{GitalyConfig.dynamic_port('tls')}"
    bin_dir = "#{build_dir}/bin"

    [tls]
    certificate_path = "#{cert_path}/gitalycert.pem"
    key_path = "#{cert_path}/gitalykey.pem"

    [gitlab-shell]
    dir = "#{GITLAB_SHELL_DIR}"

    [gitlab]
    url = 'http://gitlab_url'

    [gitaly-ruby]
    dir = "#{GITALY_RUBY_DIR}"

    [[storage]]
    name = "#{DEFAULT_STORAGE_NAME}"
    path = "#{DEFAULT_STORAGE_DIR}"
  CONFIG
  config_path = File.join(TMP_DIR, 'gitaly-rspec-config.toml')
  File.write(config_path, config_toml)

  test_log = File.join(TMP_DIR, 'gitaly-rspec-test.log')
  options = { out: test_log, err: test_log, chdir: TMP_DIR }

  gitaly_pid = spawn(File.join(build_dir, 'bin/gitaly'), config_path, options)
  at_exit { Process.kill('KILL', gitaly_pid) }

  wait_ready!(File.join(TMP_DIR, SOCKET_PATH))
end

def wait_ready!(socket)
  last_exception = StandardError.new('wait_ready! has not made any connection attempts')

  print('Booting gitaly for integration tests')
  100.times do |_i|
    sleep 0.1
    printf('.')
    begin
      UNIXSocket.new(socket).close
      puts ' ok'
      return
    rescue => ex
      last_exception = ex
    end
  end

  puts
  raise last_exception
end

start_gitaly
