# Demonstration of concatenating pack files to speed up Git clone

This directory contains code for an executable that can speed up a Git clone when installed on a server. The only type of clone we can speed up is a full clone.

- compile the executable and install at some chosen path, e.g. `go build -o /tmp/pack-objects-clone-bundle`
- `git  config --global uploadpack.packObjectsHook /tmp/pack-objects-bundle` (has to be global for some reason)
- in the bare repo you want to speed up, run `git bundle create clone.bundle --branches --tags`
- now do a full clone from that repo. If it is a local clone, use `git clone --no-local` to see the effect
