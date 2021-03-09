require 'oj'
require 'time'

# This script takes log data on standard input and simulates the results
# of different cache expiry times. The data expected is
# newline-delimited JSON (one JSON object per line) with the
# following keys:
# - timestamp
# - cache_key
# - repo_storage
# - project_path
# - stdout_bytes
# - stderr_bytes
#
# By default, it produces a row per (expiry, repo_storage) pair. To
# change the second part of that pair to be project_path, pass
# project_path as the first argument.

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
  facet_by = ARGV[0] || 'repo_storage'
  records = []

  while rec = next_record
    records << rec
  end

  records.sort_by!(&:created_at)

  puts "Expiry,#{facet_by},Hits,Misses,Hit bytes,Miss bytes,Max size"

  [2, 5, 10].each do |minutes|
    simulate(records, minutes*60, facet_by)
  end
end

def simulate(records, expiry, facet_by)
  cache = {}
  facets = {}

  records.each do |rec|
    _, first = cache.first
    while first && rec.created_at - first.created_at > expiry
      cache.shift
      facets[first.send(facet_by)][:size] -= first.size
      _, first = cache.first
    end

    facet = rec.send(facet_by)
    facets[facet] ||= Hash.new(0)

    if cache.has_key?(rec.key)
      facets[facet][:hit] += 1
      facets[facet][:hit_bytes] += rec.size
      next
    end

    cache[rec.key] = rec
    facets[facet][:miss] += 1
    facets[facet][:miss_bytes] += rec.size
    facets[facet][:size] += rec.size

    if facets[facet][:size] > facets[facet][:max_size]
      facets[facet][:max_size] = facets[facet][:size]
    end
  end

  facets.each do |key, value|
    puts [expiry, key, *value.values_at(:hit, :miss, :hit_bytes, :miss_bytes, :max_size)].join(',')
  end
end

def next_record
  line = STDIN.gets
  return unless line
  Record.new(Oj.load(line))
end

main
