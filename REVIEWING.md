# Gitaly code review process

## Roles

**contributor**: usually the creator of the merge request. They are the
[DRI](https://about.gitlab.com/handbook/people-group/directly-responsible-individuals/)
responsible for getting the MR to a merge state.

**reviewer**: the person reviewing the merge request and looking out the
guidelines in this document are followed. Each MR should be approved by at least
one reviewer.

### Choosing reviewers

When a merge request is created, Danger bot will suggest two people for
review. To spread the load across the team, it's generally recommended to assign
these two for review. But it's not uncommon to swap out one or two of them when:
- they are OOO
- they know the context of the change already

The complete list of elegible reviewers can be found at:
https://about.gitlab.com/handbook/engineering/projects/#gitaly_assignments

For small changes it's fine to only chose one person for review. The contributor
can ask the reviewer if they are okay with this.

## Criteria

The main review criteria are:

- **readability**: can humans understand the code?
- **correctness**: will a computer do the right thing when running the code?
- **do we want to make this change**: is this change the right thing to do?

The last point is easy to overlook. For example, you don't want to merge some
crystal clear water-tight piece of code that causes a production
outage, because causing an outage is not the right thing to do.

## Tips for streamlined and thorough reviews

Goals of these tips:

1. Streamline the review and acceptance process: improve throughput
2. Ensure a thorough review: minimize the number of problems that are discovered after merging

### Tips for the Contributor

As the contributor you have a dual role. You are driving the change in the MR, but you
are also the **first reviewer** of the MR. Below you will find some tips for reviewing
your own MR. Doing this costs time, but it should also save time for the other
reviewers and thereby speed up the acceptance of your MR.

#### Use the GitLab MR page to put on your "reviewer hat"

It is sometimes hard to switch from being the contributor, to being a reviewer
of your own work. Looking at your work in the GitLab UI (the merge request page)
helps to get in the reviewer mindset.

#### Reconsider the title of your MR after you reviewed it

When you are in the contributor mindset, you don't always know
what you are doing, or why. You discover this as you go along. The title you wrote for
your MR when you first pushed it is often not the best title for what is really
going on.

After you have read the diff of your MR in the GitLab UI, take a moment
to consider what the title of your MR should be, and update the MR title if needed.

Imagine your title appearing as a [CHANGELOG](CHANGELOG.md) entry.
Will your title give a good indication of what changed?

#### Your MR description should pass the lunch test

Imagine you are having lunch with a colleague and they ask you what you are
working on. You want to tell them about your MR. What do you say to them? This is
roughly what should go in your MR description.

Note that you would probably tell your colleague more than just the
title of the MR. And also more than "I'm fixing issue 1234". The
description should be a summary of _what_ is changing and _why_.

#### Leave notes on your MR as you self-review it

It may feel funny to literally talk to yourself but it works. If
thoughts occur to you as you read your MR, use the comment function
and just write them down.

You probably want to address some or most of your comments before you send your
MR to another reviewer.

#### Your MR should pass your own review before it goes to a "real" reviewer

- You have done a self-review
- The only unresolved comments from your self-review are questions you don't know
  how to answer
- The MR title clearly describes what is changing
- The MR description explains the "what" and "why", and contains issue links if
  applicable
- The MR is not in an unmergeable WIP ("work in progress") state
- GitalyBot comments have been addressed (labels, changelog etc.)
- The CI build is green

### Tips for the Reviewer

#### Use the "Start/Submit Review" feature of GitLab

The "Start/Submit Review" lets you write comments on a MR that are initially
only visible to you. Until you submit the review as a whole, you can still add,
change and remove comments.

In order to keep your review focused it is important
to be selective about what you do and do not say, and the Start/Submit feature is very
helpful for this.

#### Ask yourself if your comments are necessary

When you use the "Start/Submit Review" feature you have a unique opportunity to
take things back and leave them unsaid.

Before you submit, look at each of your comments, and ask yourself if it's
necessary to make that comment.

#### Ignore superficial problems if you spot deep problems

You finished a review round and you are about to submit your review with the
"Submit Review" feature. Look at your comments. Do some of them point at a major
problem in the MR?

For example:
- the MR is solving the wrong problem
- the MR is making a backwards incompatible change
- the MR has a test that does not test the right thing

If your review identifies both major problems and superficial problems, consider
deleting your comments about the superficial problems. The contributor should
spend their energy on the big problems first, and the code with superficial
problems might not be there anymore in the next round.

#### Expect a high standard of readability

Code that is hard to read is bad for several reasons:

- It makes the review slower because the reviewer needs more time to understand
  what is going on.
- It makes it more likely that mistakes / deep problems are hiding in the MR.
- If merged in poorly readable state, it makes all future human interactions
  with this code harder and slower. Interactions can mean: changes, addition,
  bug hunts.

For all these reasons and more it is important to flag things that are hard to
read and ask for them to be improved.

But **be honest**. Ask yourself if a "readability improvement" is really
objectively better, or just a matter of taste.

#### Be thorough in every review round

As a reviewer, it is natural to become less and less thorough in each review
round. Watch out for this. Problems can remain hidden until late in the review.

Sometimes the MR needs several rounds of readability improvements before you
find a deep problem. Ideally, the deep problem is found as early as possible,
but in practice it doesn't always work that way. If you find a deep problem or
a hard question, no matter which review round you're in, you need to bring it
up and at least discuss it.

#### Be vocal if you don't understand something

Sometimes, as a reviewer, you may feel ashamed to ask a question. You
see something in the MR you don't understand but "it's probably
something everybody knows". This is a trap.

In the context of a review, that feeling that says "I don't understand
this" is a very important signal. Put it into words, write a comment.
Think out loud about why you don't understand the thing that gives you
this feeling.

For example, if you do not understand what an RPC is supposed to do or
why it exists, ask the reviewer to
[add comments to the interface](proto/README.md#documentation).

Maybe now is the time to spend 15-30 minutes brushing up your
knowledge on this subject. Or to ask a colleague if they know more
about this. Or to bring in another reviewer who knows more about this.
Or you have stumbled on something in the MR that is wrong and that
everybody else has been overlooking.

Either way, say something.

### Conclusion

Getting your merge request through review can be hard. Reviewing
someone else's merge request can be hard. But if we hold ourselves to
a high standard, I think we can make it easier for all involved.
