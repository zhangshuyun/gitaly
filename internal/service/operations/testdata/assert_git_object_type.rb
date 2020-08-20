def assert_git_object_type!(oid, expected_type)
  out = IO.popen(%W[git cat-file -t #{oid}], &:read)
  abort 'cat-file failed' unless $?.success?

  unless out.chomp == expected_type
    abort "error: expected #{expected_type} object, got #{out}"
  end
end
