# Contributing

## Dependencies

This project uses [mise] to pin every tool it depends on. Install mise, then run `mise install`
to fetch everything declared in the `[tools]` section of `mise.toml` (currently Go, buf, and
mkcert). After that, `mise deps` syncs the pinned dev tools (golangci-lint, mockgen).

## Useful Commands

`mise` also serves as the task runner. Run `mise tasks` to list all available commands.

| Command           | Description         |
| ----------------- | ------------------- |
| `mise run deps`   | Update dependencies |
| `mise run lint`   | Lint Go files       |
| `mise run format` | Format Go files     |
| `mise run test`   | Run tests           |

[mise]: https://mise.jdx.dev/
