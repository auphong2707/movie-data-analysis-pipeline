# Makefile for Batch Layer

.PHONY: help test test-unit test-integration clean install lint format

# Default target
help:
	@echo "Batch Layer Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  install           - Install Python dependencies"
	@echo "  test              - Run unit tests"
	@echo "  test-unit         - Run unit tests only"
	@echo "  test-integration  - Run integration tests (requires services)"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  lint              - Run linters (flake8)"
	@echo "  format            - Format code with black"
	@echo "  clean             - Clean temporary files"
	@echo "  run-bronze        - Run Bronze layer ingestion"
	@echo "  run-silver        - Run Silver layer transformation"
	@echo "  run-gold          - Run Gold layer aggregation"
	@echo "  run-export        - Run MongoDB export"

# Install dependencies
install:
	pip install -r requirements.txt

# Run unit tests
test-unit:
	pytest layers/batch_layer/tests/test_*.py -v -m "not integration"

# Run integration tests (requires RUN_INTEGRATION=1)
test-integration:
	RUN_INTEGRATION=1 pytest layers/batch_layer/tests/test_integration.py -v -s

# Run all tests
test:
	pytest layers/batch_layer/tests/ -v

# Run tests with coverage
test-coverage:
	pytest layers/batch_layer/tests/ --cov=layers/batch_layer --cov-report=html --cov-report=term

# Lint code
lint:
	flake8 layers/batch_layer --max-line-length=100 --exclude=__pycache__,venv

# Format code
format:
	black layers/batch_layer --line-length=100

# Clean temporary files
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage

# Run Bronze layer ingestion
run-bronze:
	python layers/batch_layer/master_dataset/ingestion.py --pages 10

# Run Silver layer transformation
run-silver:
	python layers/batch_layer/spark_jobs/silver/run.py --execution-date $(shell date +%Y-%m-%d)

# Run Gold layer aggregation
run-gold:
	python layers/batch_layer/spark_jobs/gold/run.py --execution-date $(shell date +%Y-%m-%d)

# Run MongoDB export
run-export:
	python layers/batch_layer/batch_views/export_to_mongo.py --execution-date $(shell date +%Y-%m-%d)

# Run full pipeline (for testing)
run-pipeline: run-bronze run-silver run-gold run-export
	@echo "✓ Full pipeline completed"
