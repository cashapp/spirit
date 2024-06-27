## Minimum Requirements

Spirit requires go 1.22 or higher. MySQL version 8.0 and higher is required for performing schema changes.

## Running tests

The tests require a MySQL server to run. If you have MySQL installed locally, you can provide it as an environment variable:

```bash
MYSQL_DSN="root:mypassword@tcp(127.0.0.1:3306)/test" go test -v ./...
```

### Install dbdeployer

We use (and recommend) [dbdeployer](https://github.com/datacharmer/dbdeployer) for running the tests against multiple MySQL versions at once. For example, with MySQL 8.0.32 installed from dbdeployer:

```bash
# deploy 8.0.32
dbdeployer deploy single 8.0.32
# Running tests
MYSQL_DSN="msandbox:msandbox@tcp(127.0.0.1:8032)/test" go test -v ./...
```

If the `MYSQL_DSN` is not specified, it currently defaults to `msandbox:msandbox@tcp(127.0.0.1:8030)/test`. This may change in the future.

## Running linter

```bash
golanglint-ci run
```
