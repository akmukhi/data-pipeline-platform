.PHONY: test test-unit test-integration test-cov test-cov-html clean install-dev

# Install development dependencies
install-dev:
	pip install -e ".[dev]"
	pip install -r requirements.txt

# Run all tests
test:
	pytest

# Run unit tests only
test-unit:
	pytest tests/unit/ -m unit

# Run integration tests only
test-integration:
	pytest tests/integration/ -m integration

# Run tests with coverage
test-cov:
	pytest --cov=pipeline --cov-report=term-missing

# Run tests with HTML coverage report
test-cov-html:
	pytest --cov=pipeline --cov-report=html
	@echo "Coverage report generated in htmlcov/index.html"

# Run specific test file
test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make test-file FILE=tests/unit/test_batch_ingestor.py"; \
		exit 1; \
	fi
	pytest $(FILE)

# Clean test artifacts
clean:
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf coverage.xml
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete

# Lint code
lint:
	flake8 src/ tests/
	mypy src/

# Format code
format:
	black src/ tests/

# Type check
type-check:
	mypy src/

