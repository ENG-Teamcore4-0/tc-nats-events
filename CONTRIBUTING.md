# Contributing to TC NATS Events

Thank you for your interest in contributing to TC NATS Events! This document provides guidelines and instructions for contributing.

## Development Setup

1. Fork and clone the repository:
```bash
git clone https://github.com/your-username/tc-nats-events.git
cd tc-nats-events
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install in development mode:
```bash
make install-dev
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Follow the existing code style
- Add tests for new functionality
- Update documentation as needed

### 3. Run Tests

```bash
# Run all tests
make test

# Run only unit tests
make test-unit

# Run integration tests (requires NATS)
make test-integration
```

### 4. Check Code Quality

```bash
# Format code
make format

# Run linting
make lint

# Type checking
make type-check
```

### 5. Commit Changes

Follow conventional commit format:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test additions/changes
- `refactor:` Code refactoring
- `chore:` Maintenance tasks

Example:
```bash
git commit -m "feat: add support for custom event handlers"
```

### 6. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## Testing

### Unit Tests

Unit tests mock external dependencies:

```python
# tests/unit/test_example.py
import pytest
from unittest.mock import AsyncMock

async def test_feature():
    # Your test here
    assert True
```

### Integration Tests

Integration tests require a running NATS server:

```python
# tests/integration/test_example.py
@pytest.mark.integration
async def test_integration():
    # Your test here
    pass
```

## Code Style

- We use [Black](https://black.readthedocs.io/) for code formatting
- We use [isort](https://pycqa.github.io/isort/) for import sorting
- We use [flake8](https://flake8.pycqa.org/) for linting
- We use [mypy](http://mypy-lang.org/) for type checking

Configuration is in `pyproject.toml`.

## Documentation

- Update docstrings for any new/modified functions
- Update README.md if adding new features
- Add examples to the `examples/` directory

## Type Hints

All new code should include type hints:

```python
from typing import Optional, List, Dict, Any

async def process_events(
    events: List[Event],
    options: Optional[Dict[str, Any]] = None
) -> List[int]:
    """Process a list of events.
    
    Args:
        events: List of events to process
        options: Optional processing options
        
    Returns:
        List of sequence numbers
    """
    # Implementation
```

## Pull Request Guidelines

1. **Title**: Clear and descriptive
2. **Description**: Explain what changes were made and why
3. **Tests**: All tests must pass
4. **Documentation**: Update relevant documentation
5. **Review**: Address reviewer feedback promptly

## Questions?

Feel free to:
- Open an issue for bugs or feature requests
- Start a discussion for questions
- Contact the maintainers

Thank you for contributing!