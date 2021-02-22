require 'json'
require 'time'

class Record
  def initialize(json)
    @json = json
  end

  def key
    json_payload.fetch('cache_key')
  end

  def created_at
    @created_at ||= Time.parse(@json.fetch('timestamp'))
  end

  def size
    Integer(json_payload.fetch('stdout_bytes')) + Integer(json_payload.fetch('stderr_bytes'))
  end

  private

  def json_payload
    @json.fetch('jsonPayload')
  end
end

def main(expiry)
  cache = {}
  stats = Hash.new(0)
  records = []

  while rec = next_record
    records << rec
  end

  records.sort! { |a, b| a.created_at <=> b.created_at }

  records.each do |rec|
    _, first = cache.first
    while first && rec.created_at - first.created_at > expiry
      cache.shift
      stats[:size] -= first.size
      _, first = cache.first
    end

    if cache.has_key?(rec.key)
      stats[:hit] += 1
      next
    end

    cache[rec.key] = rec
    stats[:miss] += 1
    stats[:size] += rec.size

    if stats[:size] > stats[:max_size]
      stats[:max_size] = stats[:size]
    end
  end

  puts stats
end

def next_record
  line = STDIN.gets
  return unless line
  Record.new(JSON.parse(line))
end

main(Integer(ARGV.first))
