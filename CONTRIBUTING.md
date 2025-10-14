# Contributing to bobrapet (operator)

First off, thank you for considering contributing. Your help is appreciated.

This document provides guidelines for contributing to the operator and its docs. Please read it carefully to ensure a smooth collaboration process.

## How Can I Contribute?

### Reporting Bugs

- **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/bubustack/bobrapet/issues).
- If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/bubustack/bobrapet/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

### Suggesting Enhancements

- Open a new issue to discuss your enhancement. Clearly describe the proposed enhancement and its benefits.
- Provide code snippets, mockups, or diagrams to illustrate your idea.

### Pull Requests

- Fork the repository and create your branch from `main`.
- Ensure the test suite passes: `make test` (envtest will be installed automatically).
- Lint: `make lint` (golangci-lint via ./bin).
- Regenerate CRDs when touching API types: `make generate`.
- Submit the pull request.

## Development Workflow

### Prerequisites

- Go 1.24+
- Docker
- `make`

### Setup

1.  Fork the repository.
2.  Clone your fork: `git clone https://github.com/your_username/bobrapet.git`
3.  Navigate to the repository directory: `cd bobrapet`
4.  Build: `make build`

### Running Tests

```bash
# Run all tests
make test
```

### Commit Message Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification. This allows for automated changelog generation and semantic versioning.

Examples:
- `feat: Add support for custom retry policies`
- `fix: Correctly handle nil inputs in Process`
- `docs: Update README with new quickstart`
- `chore: Upgrade to Go 1.24`

### Code of Conduct

Participation in this project is governed by the
[Contributor Covenant Code of Conduct](./CODE_OF_CONDUCT.md). By participating,
you are expected to uphold this code. Please report unacceptable behavior to
conduct@bubustack.com.

