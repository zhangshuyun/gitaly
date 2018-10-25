require 'open3'

def run_pipeline(pipeline, dir)
  warn "#{File.basename(dir)}$ #{pipeline.map { |c| c.join(' ') }.join(' | ')}"

  statuses = Open3.pipeline(*pipeline, chdir: dir)

  statuses.all? { |s| s && s.success? }
end

def run_pipeline!(pipeline, dir)
  abort "failed" unless run_pipeline(pipeline, dir)
end

# Note: tricks with the 'dir' argument and File.basename are there only
# to make the script output prettier.
def run!(cmd, dir=nil, env={})
 abort "failed" unless run(cmd, dir, env)
end

def run(cmd, dir=nil, env={})
  dir ||= Dir.pwd
  cmd_s = cmd.join(' ')
  warn "#{File.basename(dir)}$ #{cmd_s}"
  start = Time.now
  status = system(env, *cmd, chdir: dir)
  delta = Time.now - start
  warn sprintf("time: %.3fs\n", delta) if delta > 1.0
  status
end
