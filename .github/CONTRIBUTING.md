# Contributing

## Dependencies

The following dependencies are necessary for working with this repository.

| Dependency                                  | Version | Installation                          |
| ------------------------------------------- | ------- | ------------------------------------- |
| [go](https://go.dev/doc/install)            | 1.26    | <https://go.dev/doc/install>          |
| [helm](https://helm.sh/docs/intro/install/) | 4.1.3   | <https://helm.sh/docs/intro/install/> |
| [k3d](https://k3d.io/#installation)         | 5.8.3   | <https://k3d.io/#installation>        |
| [ko](https://ko.build/install/)             | 0.18.1  | <https://ko.build/install/>           |
| [task](https://taskfile.dev/installation/)  | 3.49.1  | <https://taskfile.dev/installation/>  |

If you use [mise], you can simply run `mise install` to get all of the required dependencies.

## Useful Commands

This project uses [Task] as its task runner. You can think of it like `make`, but better. Run `task` to list all
available commands.

| Command              | Description                                    |
| -------------------- | ---------------------------------------------- |
| `task up`            | Update dependencies                            |
| `task lint`          | Lint Go files                                  |
| `task fmt`           | Format Go files                                |
| `task test`          | Run tests                                      |
| `task dev:up`        | Start a local k3d cluster and deploy the proxy |
| `task dev:down`      | Stop local services                            |
| `task helm:template` | Render the Helm chart with dev values          |

[Task]: https://taskfile.dev/
[mise]: https://mise.jdx.dev/
