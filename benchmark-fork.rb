require 'benchmark'
require 'socket'
require 'fileutils'

DEPS = %w[rugged linguist]
N = 100
FORK_SOCKET = 'fork.socket'
THREAD_SOCKET = 'thread.socket'
OID_REGEX = /^[a-f0-9]{40}$/

class ForkServer
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

    FileUtils.rm_f(FORK_SOCKET)
    l = Socket.unix_server_socket(FORK_SOCKET)

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

class ThreadServer
  def run
    DEPS.each { |dep| require(dep) }

    FileUtils.rm_f(THREAD_SOCKET)
    l = Socket.unix_server_socket(THREAD_SOCKET)

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

def main
  pids = [
    fork { ForkServer.new.run },
    fork { ThreadServer.new.run }
  ]


  Benchmark.bm(15) do |x|
    n_spawn = N / 10
    x.report("spawn (#{n_spawn}):") { n_spawn.times { benchmark_spawn } }
    x.report("fork (#{N}):") { N.times { benchmark_socket(FORK_SOCKET) } }
    x.report("thread (#{N}):") { N.times { benchmark_socket(THREAD_SOCKET) } }
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

main
