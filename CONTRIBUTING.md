# Contributing

Thank you for your interest in this HELIOS artifact. We welcome issues, design feedback,
and well-scoped pull requests.

## Before opening a substantive PR

Open an issue first. We'd rather discuss the change at design level than have you write
code that doesn't fit the architecture. For typos, small docstring fixes, or obvious bugs,
just send the PR.

## Development setup

```bash
git clone https://github.com/577-Industries/<repo>.git
cd <repo>
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pre-commit install
pytest
```

## Style

- `ruff check` and `ruff format` (config in `pyproject.toml`)
- `mypy --strict` for `src/`
- Tests required for new functionality; aim for ≥80% line coverage
- Conventional commit messages (`feat:`, `fix:`, `docs:`, `chore:`, `test:`, `refactor:`)

## Provenance discipline

Any new connector, transformation, or fused output that produces a value must emit a
`ProvenanceRecord` per [`helios-provenance-spec`](https://github.com/577-Industries/helios-provenance-spec).
PRs that produce values without a provenance record will be sent back for revision.

## License

By contributing, you agree your contributions will be licensed under the project's
Apache 2.0 license.
