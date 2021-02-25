## Gitaly Team Process

### Feature flags

Gitaly uses feature flags to safely roll out features in production. Feature
flags are part of the `context.Context` of each RPC. The `featureflag` package
will help you with flow control.

Most of this documentation assumes operations on `gitlab.com`. For
customers, an [HTTP API is available][ff-api].

In order to roll out feature flags to `gitlab.com`, you should follow
the documented rollout process below.

Once you have [developed your feature][feature-development] you [start
by creating an issue for the rollout][issue-for-feature-rollout].

The "Feature Flag Roll Out" [template for the
issue][feature-issue-template] has a checklist for the rest of the
steps.

[ff-api]: https://docs.gitlab.com/ee/api/features.html#features-flags-api
[feature-development]: https://docs.gitlab.com/ee/development/feature_flags/index.html
[issue-for-feature-rollout]: https://gitlab.com/gitlab-org/gitaly/-/issues/new?issuable_template=Feature%20Flag%20Roll%20Out
[feature-issue-template]: https://gitlab.com/gitlab-org/gitaly/-/blob/master/.gitlab/issue_templates/Feature%20Flag%20Roll%20Out.md

#### Use and limitations

Feature flags are [enabled through chatops][enable-flags] (which is
just a consumer [of the API][ff-api]). In
[`#chat-ops-test`][chan-chat-ops-test] try:

    /chatops run feature list --match gitaly_

If you get a permission error you need to request access first. That
can be done [in the `#production` channel][production-request-acl].

For Gitaly, you have to prepend `gitaly_` to your feature flag when
enabling or disabling. For example: to check if
[`gitaly_go_user_delete_tag`][chan-production] is enabled on staging
run:

    /chatops run feature get gitaly_go_user_delete_tag --staging

Note that the full set of chatops features for the Rails environment
does not work in Gitaly. E.g. the [`--user` argument does
not][bug-user-argument], neither does [enabling by group or
project][bug-project-argument].

[enable-flags]: https://docs.gitlab.com/ee/development/feature_flags/controls.html
[chan-chat-ops-test]: https://gitlab.slack.com/archives/CB2S7NNDP
[production-request-acl]: https://gitlab.slack.com/archives/C101F3796
[chan-production]: https://gitlab.com/gitlab-org/gitaly/-/issues/3371
[bug-user-argument]: https://gitlab.com/gitlab-org/gitaly/-/issues/3385
[bug-project-argument]: https://gitlab.com/gitlab-org/gitaly/-/issues/3386

### Feature flags issue checklist

The rest of this section is help for the individual checklist steps in
[the issue template][feature-issue-template]. If this is your first
time doing this you might want to first skip ahead to the help below,
you'll likely need to file some access requests.

#### Feature flag labels

The lifecycle of feature flags is monitored via issue labels.

When the issue is created from a template it'll be created with
[`featureflag::disabled`][featureflag-disabled]. Then as part of the
checklist the person rolling it out will add
[`featureflag::staging`][featureflag-staging] and
[`featureflag::production`][featureflag-production] flags to it.

[featureflag-disabled]: https://gitlab.com/gitlab-org/gitaly/-/issues?label_name[]=featureflag%3A%3Adisabled
[featureflag-staging]: https://gitlab.com/gitlab-org/gitaly/-/issues?label_name[]=featureflag%3A%3Astaging
[featureflag-production]: https://gitlab.com/gitlab-org/gitaly/-/issues?label_name[]=featureflag%3A%3Aproduction

#### Is the required code deployed?

A quick way to see if your MR is deployed is to check if [the release
bot][release-bot] has deployed it to staging, canary or production by
checking if the MR has [a `workflow::staging`][deployed-staging],
[`workflow::canary`][deployed-canary] or
[`workflow::production`][deployed-production] label.

The [/help action on gitlab.com][help-action] shows the currently
deployed hash. Copy that `HASH` and look at `GITALY_SERVER_VERSION` in
[gitlab-org/gitlab.git][gitlab-git] to see what the embedded gitaly
version is. Or in [a gitaly.git checkout][gitaly-git] run this to see
what commits aren't deployed yet:

    git fetch
    git shortlog $(curl -s https://gitlab.com/gitlab-org/gitlab/-/raw/HASH/GITALY_SERVER_VERSION)..origin/master

See the [documentation on releases below](#gitaly-releases) for more
details on the tagging and release process.

[release-bot]: https://gitlab.com/gitlab-release-tools-bot
[deployed-staging]: https://gitlab.com/gitlab-org/gitaly/-/merge_requests?state=merged&label_name=workflow%3A%3Aproduction
[deployed-canary]: https://gitlab.com/gitlab-org/gitaly/-/merge_requests?state=merged&label_name=workflow%3A%3Aproduction
[deployed-production]: https://gitlab.com/gitlab-org/gitaly/-/merge_requests?state=merged&label_name=workflow%3A%3Aproduction
[help-action]: https://gitlab.com/help
[gitlab-git]: https://gitlab.com/gitlab-org/gitlab/
[gitaly-git]: https://gitlab.com/gitlab-org/gitaly/

#### Do we need a change management issue?

#### Enable on staging

##### Prerequisites

You'll need chatops access. See [above](#use-and-limitations).

##### Steps

Run:

`/chatops run feature set gitaly_X true --staging`

Where `X` is the name of your feature.

#### Test on staging

##### Prerequisites

Access to https://staging.gitlab.com/users is not the same as on
gitlab.com (or signing in with Google on the @gitlab.com account). You
must [request access to it][staging-access-request].

As of December 2020 clicking "Sign in" on
https://about.staging.gitlab.com will redirect to https://gitlab.com,
so make sure to use the `/users` link.

As of writing signing in at [that link][staging-users-link] will land
you on the `/users` 404 page once you're logged in. You should then
typically manually modify the URL
`https://staging.gitlab.com/YOURUSER`
(e.g. https://staging.gitlab.com/avar) or another way to get at a test
repository, and manually test from there.

[staging-access-request]: https://gitlab.com/gitlab-com/team-member-epics/access-requests/-/issues/new?issuable_template=Individual_Bulk_Access_Request
[staging-users-link]: https://staging.gitlab.com/users

##### Steps

Manually use the feature in whatever way exercises the code paths
being enabled.

Then enable `X` on staging, with:

     /chatops run feature set gitaly_X --staging

##### Discussion

It's a good idea to run the feature for a full day on staging, this is
because there are daily smoke tests that run daily in that
environment. These are handled by
[gitlab-org/gitlab-qa.git][gitlab-qa-git]

[gitlab-qa-git]: https://gitlab.com/gitlab-org/gitlab-qa#how-do-we-use-it

#### Enable in production

##### Prerequisites

Have you waited enough time with the feature running in the staging
environment? Good!

##### Steps

To enable your `X` feature at 5/25/50 percent, run:

    /chatops run feature set gitaly_X 5
    /chatops run feature set gitaly_X 25
    /chatops run feature set gitaly_X 50

And then finally when you're happy it works properly do:

    /chatops run feature set gitaly_X 100

Followed by:

    /chatops run feature set gitaly_X true

Note that you need both the `100` and `true` as separate commands. See
[the documentation on actor gates][actor-gates]

If the feature is left at `50%` but is also set to `true` by default
the `50%` will win, even if `OnByDefault: true` is [set for
it](#feature-lifecycle-after-it-is-live). It'll only be 100% live once
the feature flag code is deleted. So make sure you don't skip the
`100%` step.

[actor-gates]: https://docs.gitlab.com/ee/development/feature_flags/controls.html#process

##### Discussion

What percentages should you pick and how long should you wait?

It makes sense to be aggressive about getting to 50% and then 100% as
soon as possible.

You should use lower percentages only as a paranoia check to make sure
that it e.g. doesn't spew errors at users unexpectedly at a high rate,
or (e.g. if it invokes a new expensive `git` command) doesn't create
runaway load on our servers.

But say running at 5% for hours after we've already had sufficient
data to demonstrate that we won't be spewing errors or taking down the
site just means you're delaying getting more data to be certain that
it works properly.

Nobody's better off if you wait 10 hours at 1% to get error data you
could have waited 1 hour at 10% to get, or just over 10 minutes with
close monitoring at 50%.

#### Feature lifecycle after it is live

##### Discussion

After a feature is running at `100%` for what ever's deemed to be a
safe amount of time we should change it to be `OnByDefault: true`. See
[this MR for an example][example-on-by-default-mr].

We should add a changelog entry when `OnByDefault: true` is flipped.

That should then be followed up by another MR to remove the
pre-feature code from the codebase, and we should add another
changelog entry when doing that.

This is because even after setting `OnByDefault: true` users might
still have opted to disable the new feature. See [the discussion
below](#two-phase-ruby-to-go-rollouts)) for possibly needing to do
such changes over multiple releases.

[example-on-by-default-mr]: https://gitlab.com/gitlab-org/gitaly/-/merge_requests/3033

##### Two phase Ruby to Go rollouts

Depending on what the feature does it may be bad to remove the `else`
branch where we have the feature disabled at this point. E.g. if it's
a rewrite of Ruby code in Go.

As we deploy the Ruby code might be in the middle of auto-restarting,
so we could remove its code before the Go code has a chance to update
with its default, and would still want to call it. So therefore you
need to do any such removal in two gitlab.com release cycles.

See the example of [MR !3033][example-on-by-default-mr] and [MR
!3056][example-post-go-ruby-code-removal-mr] for how to do such a
two-phase removal.

[example-on-by-default-mr]: https://gitlab.com/gitlab-org/gitaly/-/merge_requests/3033
[example-post-go-ruby-code-removal-mr]: https://gitlab.com/gitlab-org/gitaly/-/merge_requests/3056

##### Remove the feature flag via chatops

After completing the above steps the feature flag should be deleted
from the database of available features via `chatops`.

If you don't do this others will continue to see the features with
e.g.:

	/chatops run feature list --match=gitaly_

It also incrementally adds to data that needs to be fetched &
populated on every request.

To remove the flag first sanity check that it's the feature you want,
that it's at [`100%` and is `true`](#enable-in-production):

	/chatops run feature get gitaly_X

Then delete it if that's the data you're expecting:

	/chatops run feature delete gitaly_X

### Gitaly Releases

Gitaly releases are tagged automatically by
[`release-tools`][release-tools] when a Release Manager tags a GitLab
version.

[release-tools]: https://gitlab.com/gitlab-org/release-tools

#### Major or minor releases

Once we release GitLab X.Y.0, we also release gitaly X.Y.0 based on the content of `GITALY_SERVER_VERSION`.
This version file is automatically updated by `release-tools` during auto-deploy picking.

Because gitaly master is moving we need to take extra care of what we tag.

Let's imagine a situation like this on `master`

```mermaid
graph LR;
  A-->b0;
  A-->B;
  b0:::branch-->b1;
  b1:::branch-->B;
  B-->C;
  B-->c0;
  c0:::branch-->C;
  classDef branch fill:#f96;
  classDef tag fill:yellow;
```

Commit `C` is picked into auto-deploy and the build is successfully deployed to production

We are ready to tag `v12.9.0` but there is a new merge commit, `D`, on gitaly `master`.

```mermaid
graph LR;
  A-->b0;
  A-->B;
  b0:::branch-->b1;
  b1:::branch-->B;
  B-->C;
  B-->c0;
  c0:::branch-->C;
  C-->D;
  C-->d0;
  d0:::branch-->D
  classDef branch fill:#f96;
  classDef tag fill:yellow;
```

We cannot tag on `D` as it never reached production.

`release-tools` follows this algorithm:
1. create a stable branch from `GITALY_SERVER_VERSION` (commit `C`),
1. bump the version and
1. prepare the changelog (commit `C'`).

Then we tag this commit and we merge back to `master`

```mermaid
graph LR;
  A-->b0;
  A-->B;
  b0:::branch-->b1;
  b1:::branch-->B;
  B-->C;
  B-->c0;
  c0:::branch-->C;
  C-->D;
  C-->d0;
  d0:::branch-->D
  C-->C';
  id1>v12.9.0]:::tag-->C';
  D-->E;
  C':::stable-->E;
  classDef branch fill:#f96;
  classDef tag fill:yellow;
  classDef stable fill:green;
```

Legend
```mermaid
graph TD;
  A["master commit"];
  b0["feature branch commit"]:::branch;
  id1>tag]:::tag;
  C["stable branch commit"]:::stable;
  classDef branch fill:#f96;
  classDef tag fill:yellow;
  classDef stable fill:green;
```

With this solution, the team can autonomously tag any RC they like, but the other releases are handled by the GitLab tagging process.

#### Patch releases

The Gitaly team usually works on patch releases in the context of a security release.

The release automation creates the stable branches, tagging the stable branch is automated in `release-tools` as well.
A Gitaly maintainer will only take care of merging the fixes on the stable branch.

For patch releases, we don't merge back to master. But `release-tools` will commit a changelog update to both the patch release, and the master branch.

#### Creating a release candidate

Release candidate (RC) can be created with a chatops command.
This is the only type of release that a developer can build autonomously.

When working on a GitLab feature that requires a minimum gitaly version,
tagging a RC is a good way to make sure the gitlab feature branch has the proper gitaly version.

- Pick the current milestone (i.e. 12.9)
- Pick a release candidate number, you can check `VERSION` to see if we have one already (12.9.0-rc1)
- run `/chatops run gitaly tag 12.9.0-rc1`
- The release will be published
- The [pipeline of a tag](https://gitlab.com/gitlab-org/gitaly/pipelines?scope=tags&page=1)
  has a **manual** job, `update-downstream-server-version`, that will create a merge request on the GitLab codebase to bump the Gitaly server version, and this will be assigned to you.
  Once the build has completed successfully, assign it to a maintainer for review.

### Publishing the ruby gem

If an updated version of the ruby proto gem is needed, it can be published to rubygems.org with the `_support/publish-gem` script.

If the changes needed are not yet released, [create a release candidate](#creating-a-release-candidate) first.

- Checkout the tag to publish (vX.Y.Z)
- run `_support/publish-gem X.Y.Z`

##### Security release

Security releases involve additional processes to ensure that recent releases
of GitLab are properly patched while avoiding the leaking of the security
details to the public until appropriate.

Before beginning work on a security fix, open a new Gitaly issue with the template
`Security Release` and follow the instructions at the top of the page for following
the template.

## Experimental builds

Push the release tag to dev.gitlab.org/gitlab/gitaly. After
passing the test suite, the tag will automatically be built and
published in https://packages.gitlab.com/gitlab/unstable.
