# CLAUDE.md

## Git Workflow

### Branch and PR Requirements

- **NEVER** push changes directly to `main` branch
- **ALWAYS** create a new branch for changes
- **ALWAYS** submit a proper Pull Request for code review
- Branch naming convention: Use descriptive names like `username/feature-name`

### Commit Requirements

- When possible, use `[pkg]: <title cased change>` for the first line of the commit message. E.g. `[server]: Add
HealthCheck Logic`
- No need to add co-author attribution for Claude
- Always run `task fmt` before committing

## Code Style & Conventions

### Linting

- **ALWAYS** ensure code passes linting before committing (use `task lint` to check)
- Fix any linting errors before pushing commits (use `task fmt` before manually updating)

### Import Formatting

- Follow `gci` (goimports-reviser) formatting rules:
  1. Standard library imports
  2. Third-party imports
  3. Project imports (github.com/temporalio/s2s-proxy/...)

- Add blank lines between import groups
- Running `task fmt` will do this for you

## Useful Commands

```sh
task test              # Run all tests with coverage
task lint              # Lint Go files
task fmt               # Format Go files (gofmt + gci import ordering)
task up                # Update dependencies (go mod tidy + tools)

# Run a single test
go test ./internal/server/... -run TestServerStart
```

## Architecture

This is a gRPC proxy for Temporal. The entry point is `cmd/proxy/main.go`, which wires everything together using [Uber
fx](https://github.com/uber-go/fx) for dependency injection.

## Testing Patterns

Tests use `testify` (require, not assert) and `t.Parallel()` throughout.

## Import Ordering

`gci` enforces three import groups in order: standard library → third-party → local module
(`github.com/temporalio/temporal-proxy`). Running `task fmt` fixes this automatically.
