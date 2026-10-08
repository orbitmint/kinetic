# Contributing to Kinetic

Thank you for your interest in contributing to **Kinetic**! We welcome bug reports, documentation improvements, and feature contributions.

## Developing Locally

### Prerequisites
* Rust 1.80+ (recommended stable toolchain)
* Docker & Docker Compose (for local test clusters)

### Running Tests
Make sure the test suite passes before submitting PRs:

```bash
cargo test
```

### Checking Formats and Warnings
```bash
cargo fmt -- --check
cargo clippy -- -D warnings
```

## Pull Request Guidelines

1. **Keep PRs Focused:** Small, single-purpose pull requests are preferred.
2. **Add Tests:** Any new CDC feature, codec, or schema mapping should include a unit test verifying its correctness.
3. **Update Docs:** If you modify CLI flags or environment variables, update the `README.md` and landing page docs accordingly.

## Code of Conduct
We adhere to standard open-source community standards: be respectful, constructive, and collaborative.
