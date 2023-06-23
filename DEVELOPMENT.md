# Setup
Spirit uses [hermit](https://cashapp.github.io/hermit/usage/get-started/) to enforce tool versions across environments.
Install hermit in your machine, and it will make sure you are using the correct version of all tools.


## Running tests

#### Install dbdeployer
_TODO: Add support for hermit package_

We use https://github.com/datacharmer/dbdeployer to run different versions of MySQL and run tests against those versions.

```bash
 # Install and setup
curl -s https://raw.githubusercontent.com/datacharmer/dbdeployer/master/scripts/dbdeployer-install.sh | bash
dbdeployer init
dbdeployer deploy single 8.0 --force --port 8030
```

```bash
# Running tests
MYSQL_DSN="msandbox:msandbox@tcp(127.0.0.1:8030)/test" go test -v ./...
```

## Running linter

```bash
golanglint-ci run
```