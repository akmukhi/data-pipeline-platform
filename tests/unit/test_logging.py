"""Unit tests for logging utility."""

import pytest

from pipeline.utils.logging import (
    PipelineLogger,
    generate_correlation_id,
    get_logger,
    get_pipeline_id,
    get_pipeline_stage,
    set_correlation_id,
    set_pipeline_id,
    set_pipeline_stage,
    setup_logging,
)


@pytest.mark.unit
class TestLogging:
    """Test logging utilities."""

    def test_generate_correlation_id(self):
        """Test correlation ID generation."""
        cid1 = generate_correlation_id()
        cid2 = generate_correlation_id()

        assert cid1 != cid2
        assert len(cid1) > 0

    def test_set_get_correlation_id(self):
        """Test setting and getting correlation ID."""
        cid = generate_correlation_id()
        set_correlation_id(cid)

        assert get_correlation_id() == cid

    def test_set_get_pipeline_id(self):
        """Test setting and getting pipeline ID."""
        pipeline_id = "test_pipeline_001"
        set_pipeline_id(pipeline_id)

        assert get_pipeline_id() == pipeline_id

    def test_set_get_pipeline_stage(self):
        """Test setting and getting pipeline stage."""
        stage = "ingestion"
        set_pipeline_stage(stage)

        assert get_pipeline_stage() == stage

    def test_get_logger(self):
        """Test getting logger."""
        logger = get_logger(__name__)
        assert isinstance(logger, PipelineLogger)

    def test_pipeline_logger(self):
        """Test PipelineLogger."""
        logger = get_logger(__name__)

        # Should not raise
        logger.debug("Debug message")
        logger.info("Info message")
        logger.warning("Warning message")
        logger.error("Error message")

    def test_pipeline_logger_with_stage(self):
        """Test PipelineLogger with stage context."""
        logger = get_logger(__name__)

        with logger.with_stage("ingestion"):
            assert get_pipeline_stage() == "ingestion"
            logger.info("In ingestion stage")

        assert get_pipeline_stage() != "ingestion"

    def test_pipeline_logger_with_pipeline(self):
        """Test PipelineLogger with pipeline context."""
        logger = get_logger(__name__)

        with logger.with_pipeline("pipeline_001"):
            assert get_pipeline_id() == "pipeline_001"
            logger.info("In pipeline")

        assert get_pipeline_id() != "pipeline_001"

    def test_setup_logging(self):
        """Test logging setup."""
        setup_logging(level="DEBUG", format_type="text")
        # Should not raise

