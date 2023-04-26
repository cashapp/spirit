# Setup
Spirit uses [hermit](https://cashapp.github.io/hermit/usage/get-started/) to enforce tool versions across environments.
Install hermit in your machine, and it will make sure you are using the correct version of all tools.

## Running tests

```bash
go test ./...
```

## Running linter

```bash
golanglint-ci run
```