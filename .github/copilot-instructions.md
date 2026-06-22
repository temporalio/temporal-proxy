# Copilot Code Review Instructions

This is a gRPC proxy for Temporal. Use these guidelines when reviewing pull
requests. Flag the issues below; each rule includes the reasoning so you can
apply it sensibly rather than mechanically.

## Naming & Go Idioms

- Omit `Get` prefixes from getters (`Status()`, not `GetStatus()`). Idiomatic Go
  reserves `Get` for calls with side effects or I/O.
- Avoid `Impl` suffixes on types. Name the concrete type for what it is, or keep
  it unexported when it only backs an interface.
- Avoid name stutter within a package (`server.New`, not `server.NewServer`;
  `config.Config` is acceptable when the type genuinely is "the config").
- Don't use underscores in test names (`TestServerStart`, not `Test_ServerStart`).
- Exported symbols need doc comments that start with the symbol name. Match the
  terse style already in the codebase; don't add needless prose.
- In general, packages should accept _interfaces_ and return _structs_ (interfaces belong in the package that needs
  them.)

## Avoid Needless Abstraction

- Don't introduce single-use constants for strings or numbers that appear once.
  Inline the literal; a constant only earns its place when reused or when the
  name adds real meaning.
- Don't add wrapper types or interfaces with a single implementation and no test
  seam that needs them. Prefer the concrete type until a second caller appears.
- Favour explicit code over reflection.
- Don't export symbols that callers outside the package never use. Keep the
  surface area small.

## Error Handling

- Wrap errors with context using `fmt.Errorf("...: %w", err)` so the chain stays
  inspectable with `errors.Is`/`errors.As`. The codebase relies on this (e.g.
  unwrapping `validation.Errors` from a wrapped config error).
- Return errors from library code; don't `panic` for conditions a caller could
  ever reasonably hit.
- Prefer standard error types and sentinel values over bespoke error structs
  unless callers need to branch on structured fields.
- Validate inputs early and fail before allocating or binding resources, rather
  than partway through construction.

## fx / Dependency Injection

The app is wired with [uber-go/fx](https://github.com/uber-go/fx). Entry point is `cmd/proxy/main.go`; each package
exposes a `Module`.

- A package's `Module` should only provide or invoke what that package owns.
  Don't construct or provide types that belong to another package; depend on
  them through the `fx.In` params instead.
- fx wiring is code, so it gets tested. Every `Module` should have a matching
  `fx_test.go` that builds an `fx.App`, runs the lifecycle (`Start`/`Stop`), and
  exercises the failure paths (invalid config, bind errors). See
  `internal/server/fx_test.go` for the pattern.
- Mark genuinely optional dependencies `optional:"true"` and fall back to a
  sensible default. Required dependencies stay required so missing wiring fails
  at app construction, not at runtime.
- Collect dependencies in an `fx.In` params struct. Use named tags
  (`name:"configFile"`) to disambiguate primitives like strings; don't inject
  bare ambiguous types.
- Bind resource lifecycles with `fx.Hook` (`OnStart`/`OnStop`). Don't open
  sockets, spawn long-lived goroutines, or acquire resources outside the
  lifecycle, since they won't be cleaned up on shutdown.
- Validate configuration in the `Invoke`/`Provide` before constructing the real
  thing, so bad input surfaces at build/start time with a clear error.

## Testing

- Use `testify/require`, not `assert`. A failed precondition should stop the
  test rather than cascade into confusing follow-on failures.
- Call `t.Parallel()` in tests and subtests unless shared state makes it unsafe;
  flag tests that omit it without reason.
- Put tests in a separate `_test` package (`server_test`) when feasible, so they
  exercise the exported surface the way real callers do.
- Prefer table-driven tests when cases share a shape; don't force a table when
  cases are genuinely different.
- Use `t.Context()` instead of `context.Background()` in tests so cancellation is
  tied to the test lifecycle.
- Don't use `time.Sleep` to order concurrent operations. Use synchronization
  primitives, and reach for `testing/synctest` when testing concurrency.
- Test only what the change needs. Flag redundant assertions, unnecessary setup,
  and over-mocked tests that assert on implementation details.

## Concurrency & Safety

- Default to `sync.Mutex` over atomics or `sync.RWMutex`; reach for the others
  only when a measured contention problem justifies the added complexity.
- Don't hold a lock across a blocking call (I/O, channel send, downstream RPC).
- Pair every channel receive that could block with a `context`/cancellation path
  so goroutines can't leak on shutdown.
- Prefer immutable data passed by value over shared mutable state.

## Consistency with the Codebase

- Follow patterns already established here: fx for wiring, the `pkg/validation`
  combinators (`Field`, `Rule`, `Check`) for input checks, `doc.go` for
  package-level docs, and the helpers in `pkg/testutil` for test fixtures.
- Match the existing import grouping (stdlib, third-party, local) and formatting.
  `mise run format` enforces it; flag diffs that bypass it.
- When adding something new, look for a comparable existing implementation and
  mirror its structure rather than introducing a parallel convention.
