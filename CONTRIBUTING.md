# Contributing

- Keep the storage layer's Redis calls atomic where correctness depends on
  it (claim/reap use Lua scripts for this reason) — don't split a
  check-then-act across two round trips.
- Every behavior change needs a test that fails without it.
- Run `pytest -q` and `ruff check .` before opening a PR; CI runs the same
  two commands against a real Redis service container.
- Keep the README's "Measured vs Claimed" table honest — update it in the
  same PR as whatever it's reporting on, not later.
