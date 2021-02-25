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

def main
  records = []

  while rec = next_record
    records << rec
  end

  [1, 5, 10, 30, 60].each do |minutes|
    simulate(records, minutes*60)
  end
end

def simulate(records, expiry)
  cache = {}
  stats = Hash.new(0)
  frequencies = Hash.new(0)

  puts "Expiry: #{expiry}s"
  records.each do |rec|
    _, first = cache.first
    while first && rec.created_at - first[:rec].created_at > expiry
      cache.shift
      frequencies[first[:frequency]] += 1
      stats[:size] -= first[:rec].size
      _, first = cache.first
    end

    if cache.has_key?(rec.key)
      cache[rec.key][:frequency] += 1
      stats[:hit] += 1
      next
    end

    cache[rec.key] = { rec: rec, frequency: 1 }
    stats[:miss] += 1
    stats[:size] += rec.size

    if stats[:size] > stats[:max_size]
      stats[:max_size] = stats[:size]
    end
  end

  cache.values.each do |value|
    frequencies[value[:frequency]] += 1
  end

  puts frequencies.sort_by(&:first).to_h
  puts stats
  puts "hit ratio: #{Float(stats[:hit])/records.size}"
  puts "max cache size: #{Float(stats[:max_size])/(1024*1024*1024)}GB"  # puts cache.values
end

def increment_last(array)
  array[0..-2] + [array.last + 1]
end

def next_record
  line = STDIN.gets
  return unless line
  Record.new(JSON.parse(line))
end

main
