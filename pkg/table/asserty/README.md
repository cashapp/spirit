# What is this?

Asserty is a package that provides a set of functions to help ensure that a database schema conforms to your expectations.

Sure, you could probably use it in tests. But our goal for this is to use it as part of service readiness checks. We want to make sure that the database schema is what we expect it to be before we start to serve traffic.