# gitaly-pack-objects

This is a **beta** pack-objects hook that can speed up a Git clone when
installed on a server. The only type of clone we can speed up is a full
clone.

Also see https://gitlab.com/groups/gitlab-org/-/epics/1117.

-   compile the executable and install at some chosen path
-   `git  config --global uploadpack.packObjectsHook /path/to/gitaly-pack-objects`
    (confighas to be global for some reason)
-   in the bare repo you want to speed up, run
    `mkdir -p gitaly && git bundle create gitaly/clone.bundle --branches --tags`
-   now do a full clone from that repo. If it is a local clone, use
    `git clone --no-local` to see the effect
