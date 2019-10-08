require 'benchmark'
require 'socket'
require 'fileutils'

DEPS = %w[rugged linguist]
N = 100
FORK_SOCKET = 'fork.socket'
PRE_FORK_SOCKET = 'pre-fork.socket'
THREAD_SOCKET = 'thread.socket'
OID_REGEX = /^[a-f0-9]{40}$/

class ForkServer < Struct.new(:socket_path)
  def run
    DEPS.each { |dep| require(dep) }

    # Every request creates a child process which becomes a zombie when it's
    # done. It's our job to reap them.
    Thread.new do
      loop do
        Process.wait
      rescue
        sleep 0.1
      end
    end

    FileUtils.rm_f(socket_path)
    l = Socket.unix_server_socket(socket_path)

    loop do
      conn, _addrinfo = l.accept

      fork do
        l.close
        repo = Rugged::Repository.new('.')
        conn.write(repo.head.target.oid)
      end

      conn.close
    end
  end
end

class ThreadServer < Struct.new(:socket_path)
  def run
    DEPS.each { |dep| require(dep) }

    FileUtils.rm_f(socket_path)
    l = Socket.unix_server_socket(socket_path)

    loop do
      conn, _addrinfo = l.accept

      Thread.new do
        repo = Rugged::Repository.new('.')
        conn.write(repo.head.target.oid)
        repo.close # simulate cleanup, needed in multi-threaded process
        conn.close
      end
    end
  end
end

class PreForkServer < Struct.new(:socket_path)
  WORKERS = 2
  REUSE = 10

  def run
    DEPS.each { |dep| require(dep) }

    FileUtils.rm_f(socket_path)
    listener = Socket.unix_server_socket(socket_path)

    WORKERS.times.map { fork { handle(listener, Process.pid) } }

    loop do
      Process.wait
      fork { handle(listener, Process.pid) }
    end
  end

  def handle(listener, ppid)
    can_exit = false
    mu = Mutex.new

    Thread.new do
      loop do
        sleep 1

        begin
          Process.kill(0, ppid)
        rescue
          mu.synchronize { exit if can_exit }
        end
      end
    end
          
     
    REUSE.times do
      mu.synchronize { can_exit = true }
      conn, _addrinfo = listener.accept
      mu.synchronize { can_exit = false }

      repo = Rugged::Repository.new('.')
      conn.write(repo.head.target.oid)
      conn.close
    end
  end
end

def main
  pids = [
    fork { ForkServer.new(FORK_SOCKET).run },
    fork { ThreadServer.new(THREAD_SOCKET).run },
    fork { PreForkServer.new(PRE_FORK_SOCKET).run },
  ]

  [FORK_SOCKET, THREAD_SOCKET, PRE_FORK_SOCKET].each do |sock|
    try_connect(sock)
  end

  Benchmark.bm(15) do |x|
    n_spawn = N / 10
    x.report("pre-fork (#{N}):") { N.times { benchmark_socket(PRE_FORK_SOCKET) } }
    x.report("fork (#{N}):") { N.times { benchmark_socket(FORK_SOCKET) } }
    x.report("thread (#{N}):") { N.times { benchmark_socket(THREAD_SOCKET) } }
    x.report("spawn (#{n_spawn}):") { n_spawn.times { benchmark_spawn } }
  end

ensure
  pids.each { |pid| Process.kill('KILL', pid) }
end

def benchmark_spawn
  cmd = ['ruby', *DEPS.map { |d| "-r#{d}" }, '-e', <<~SCRIPT
      repo = Rugged::Repository.new('.')
      print repo.head.target.oid
    SCRIPT
  ]
  out = IO.popen(cmd, 'r', &:read)
  raise 'command failed' unless $?.success?
  raise "bad output #{out.inspect}" unless OID_REGEX =~ out
end

def benchmark_socket(socket)
  Socket.unix(socket) do |conn|
    out = conn.read
    raise "bad output #{out.inspect}" unless OID_REGEX =~ out
  end
end

def try_connect(sock)
  500.times do
    begin
      UNIXSocket.new(sock).read
      return
    rescue Errno::ENOENT, Errno::ECONNREFUSED
      sleep 0.1
    end
  end

  UNIXSocket.new(sock).read
end

main
