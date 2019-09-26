require 'benchmark'
require 'socket'
require 'fileutils'

DEPS = %w[rugged linguist]
N = 10
SOCKET = 'fork.socket'

class Server
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

    FileUtils.rm_f(SOCKET)
    l = Socket.unix_server_socket(SOCKET)

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

def main
  fork { Server.new.run }

  puts "reps: #{N}"
  puts
  Benchmark.bm(7) do |x|
    x.report('spawn:') { N.times { benchmark_spawn } }
    x.report('fork:') { N.times { benchmark_fork } }
  end
end

def benchmark_spawn
  cmd = ['ruby', *DEPS.map { |d| "-r#{d}" }, '-e', <<~SCRIPT
      repo = Rugged::Repository.new('.')
      print repo.head.target.oid
    SCRIPT
  ]
  out = IO.popen(cmd, 'r', &:read)
  raise 'command failed' unless $?.success?
  raise "bad output #{out.inspect}" unless /^[a-f0-9]{40}$/ =~ out
end

def benchmark_fork
  Socket.unix(SOCKET) do |conn|
    out = conn.read
    raise "bad output #{out.inspect}" unless /^[a-f0-9]{40}$/ =~ out
  end
end

main
