require 'oj'
require 'time'

class Record
  attr_reader :created_at, :key, :size, :repo_storage, :project_path

  def initialize(json)
    @created_at = Time.parse(json.fetch('timestamp'))
    @key = json.fetch('cache_key')
    @size = Integer(json.fetch('stdout_bytes')) + Integer(json.fetch('stderr_bytes'))
    @repo_storage = json.fetch('repo_storage')
    @project_path = json.fetch('project_path')
  end
end

def main
  records = []

  while rec = next_record
    records << rec
  end

  [2, 5, 10].each do |minutes|
    simulate(records, minutes*60)
  end
end

def simulate(records, expiry)
  cache = {}
  stats = Hash.new(0)

  puts "Expiry: #{expiry}s"
  records.each do |rec|
    _, first = cache.first
    while first && rec.created_at - first[:rec].created_at > expiry
      cache.shift
      stats[:size] -= first[:rec].size
      _, first = cache.first
    end

    if cache.has_key?(rec.key)
      stats[:hit] += 1
      next
    end

    cache[rec.key] = { rec: rec }
    stats[:miss] += 1
    stats[:size] += rec.size

    if stats[:size] > stats[:max_size]
      stats[:max_size] = stats[:size]
    end
  end

  puts stats
  puts "hit ratio: #{Float(stats[:hit])/records.size}"
  puts "max cache size: #{Float(stats[:max_size])/(1024*1024*1024)}GB"  # puts cache.values
end

def next_record
  line = STDIN.gets
  return unless line
  Record.new(Oj.load(line))
end

main
