# Contributing

We're excited to hear that you are interested in contributing to Spirit!

Before you spend too much time, we encourage you to read our _philosophy_ and _housekeeping_ rules that are laid out in this document.

## Philosophy

Because the _consequences_ of bugs in Spirit are serious (data loss), we may be hesitant to add new features that do not overlap with our use case. This is because:

1. These features will naturally receive less testing.
2. They could complicate code leading to the introduction of other bugs.
3. They may also paint us into a corner, because a future feature that we want to introduce must account for a feature that we do not use.

We require new features to be _safe_ and _designed to be enabled by default_:

* By _safe_, we mean that the feature is not an optimization that could cause potential data loss. Since our workloads are for financial systems, we do not have a use-case for such optimizations.
* By _enabled by default_, our experience with previous projects is that non-default configuration options are not always well tested by test-suites and testing interactions between configuration settings is difficult. We also believe in the [Wordpress philosophy](https://wordpress.org/about/philosophy/) of "Decisions, not options".

We apologize in advance that _our use case_ may seem arbitrary, because we do in-fact have multiple use cases. But the general rule is: AWS, MySQL 8.0 (Aurora), only InnoDB, no read-replicas. Please create an issue first to discuss your new feature so that we can confirm that it is a good fit.

## Housekeeping

In terms of housekeeping:

1. Please create a new issue for your feature or bug.
2. After some initial discussion, go ahead and fork and clone the spirit repository.
3. Link to the original issue in your PR request.
4. Your PR request should contain new tests for your code, including any documentation.
5. Try and keep your PR as focused as possible. If there are multiple changes you would like to make that are not dependent upon each other, consider submitting them as separate pull requests.