"""
Data Quality validation using Great Expectations.

Provides integration with Great Expectations for data quality checks
at each layer of the batch pipeline.
"""

import os
from typing import Optional, Dict, Any, List
from datetime import datetime
import json

try:
    from pyspark.sql import DataFrame, SparkSession
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.checkpoint import SimpleCheckpoint
except ImportError:
    # Allow import without Great Expectations for documentation
    pass

from .logger import get_logger

logger = get_logger(__name__)


class DataQualityValidator:
    """
    Data quality validation using Great Expectations.
    
    Features:
    - Schema validation
    - Completeness checks
    - Range validation
    - Duplicate detection
    - Custom validation rules
    """
    
    def __init__(
        self,
        context_root_dir: Optional[str] = None,
        spark: Optional[SparkSession] = None
    ):
        """
        Initialize Data Quality Validator.
        
        Args:
            context_root_dir: Root directory for GE context (default: ./ge_expectations)
            spark: SparkSession for Spark DataFrame validation
        """
        if context_root_dir is None:
            context_root_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "config", "expectations"
            )
        
        self.context_root_dir = context_root_dir
        self.spark = spark
        
        # Initialize or get existing context
        try:
            self.context = gx.get_context(context_root_dir=context_root_dir)
        except Exception as e:
            logger.warning(f"Could not load GE context, will create on first use: {e}")
            self.context = None
    
    def validate_dataframe(
        self,
        df: DataFrame,
        expectation_suite_name: str,
        batch_identifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Validate a Spark DataFrame against an expectation suite.
        
        Args:
            df: Spark DataFrame to validate
            expectation_suite_name: Name of the expectation suite
            batch_identifier: Identifier for this batch (default: timestamp)
        
        Returns:
            Validation result dictionary
        
        Raises:
            ValueError: If validation fails critically
        """
        if batch_identifier is None:
            batch_identifier = datetime.utcnow().isoformat()
        
        logger.info(
            f"Validating DataFrame with suite: {expectation_suite_name}",
            extra={"context": {
                "suite": expectation_suite_name,
                "batch_id": batch_identifier,
                "row_count": df.count()
            }}
        )
        
        try:
            # Create runtime batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="spark_datasource",
                data_connector_name="runtime_data_connector",
                data_asset_name="batch_dataframe",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"batch_id": batch_identifier}
            )
            
            # Run validation
            checkpoint_config = {
                "name": f"checkpoint_{expectation_suite_name}",
                "config_version": 1.0,
                "class_name": "SimpleCheckpoint",
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": expectation_suite_name
                    }
                ]
            }
            
            checkpoint = SimpleCheckpoint(
                f"checkpoint_{expectation_suite_name}",
                self.context,
                **checkpoint_config
            )
            
            result = checkpoint.run()
            
            # Extract validation results
            success = result.success
            statistics = result.run_results[list(result.run_results.keys())[0]]
            
            result_summary = {
                "success": success,
                "batch_id": batch_identifier,
                "suite_name": expectation_suite_name,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "statistics": {
                    "evaluated_expectations": statistics.statistics["evaluated_expectations"],
                    "successful_expectations": statistics.statistics["successful_expectations"],
                    "unsuccessful_expectations": statistics.statistics["unsuccessful_expectations"],
                    "success_percent": statistics.statistics["success_percent"]
                }
            }
            
            if success:
                logger.info(
                    f"Validation passed: {expectation_suite_name}",
                    extra={"context": result_summary}
                )
            else:
                logger.error(
                    f"Validation failed: {expectation_suite_name}",
                    extra={"context": result_summary}
                )
            
            return result_summary
            
        except Exception as e:
            logger.error(
                f"Validation error for suite {expectation_suite_name}: {str(e)}",
                exc_info=True
            )
            raise
    
    def create_bronze_expectations(self, suite_name: str = "bronze_suite") -> str:
        """
        Create expectation suite for Bronze layer.
        
        Bronze layer expectations:
        - Schema compliance (required columns exist)
        - Non-null critical fields (movie_id, extraction_timestamp)
        - Valid data types
        
        Returns:
            Name of created suite
        """
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        # Add expectations
        expectations = [
            # Columns exist
            {"expectation_type": "expect_table_columns_to_match_ordered_list",
             "kwargs": {
                 "column_list": ["movie_id", "raw_data", "extraction_timestamp", 
                                "partition_year", "partition_month", "partition_day", "partition_hour"]
             }},
            
            # Non-null critical fields
            {"expectation_type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "movie_id"}},
            {"expectation_type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "extraction_timestamp"}},
            
            # Data type checks
            {"expectation_type": "expect_column_values_to_be_of_type",
             "kwargs": {"column": "movie_id", "type_": "IntegerType"}},
        ]
        
        for exp in expectations:
            suite.add_expectation(**exp)
        
        self.context.save_expectation_suite(suite)
        logger.info(f"Created Bronze expectation suite: {suite_name}")
        
        return suite_name
    
    def create_silver_expectations(self, suite_name: str = "silver_suite") -> str:
        """
        Create expectation suite for Silver layer.
        
        Silver layer expectations:
        - No duplicate movie_ids
        - Data quality > 95% (non-null values)
        - Valid ranges for numeric fields
        - Valid date formats
        
        Returns:
            Name of created suite
        """
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        expectations = [
            # No duplicates
            {"expectation_type": "expect_compound_columns_to_be_unique",
             "kwargs": {"column_list": ["movie_id"]}},
            
            # Completeness checks (>95% non-null)
            {"expectation_type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "title", "mostly": 0.95}},
            {"expectation_type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "release_date", "mostly": 0.95}},
            
            # Range validations
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "vote_average", "min_value": 0.0, "max_value": 10.0}},
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "vote_count", "min_value": 0}},
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "popularity", "min_value": 0.0}},
            
            # Sentiment score range
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "sentiment_score", "min_value": -1.0, "max_value": 1.0, "mostly": 0.95}},
        ]
        
        for exp in expectations:
            suite.add_expectation(**exp)
        
        self.context.save_expectation_suite(suite)
        logger.info(f"Created Silver expectation suite: {suite_name}")
        
        return suite_name
    
    def create_gold_expectations(self, suite_name: str = "gold_suite") -> str:
        """
        Create expectation suite for Gold layer.
        
        Gold layer expectations:
        - Aggregation completeness
        - Valid metric ranges
        - Non-negative counts and sums
        
        Returns:
            Name of created suite
        """
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        expectations = [
            # Non-null aggregated fields
            {"expectation_type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "total_movies"}},
            {"expectation_type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "avg_rating"}},
            
            # Non-negative counts
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "total_movies", "min_value": 0}},
            
            # Valid average ranges
            {"expectation_type": "expect_column_values_to_be_between",
             "kwargs": {"column": "avg_rating", "min_value": 0.0, "max_value": 10.0}},
        ]
        
        for exp in expectations:
            suite.add_expectation(**exp)
        
        self.context.save_expectation_suite(suite)
        logger.info(f"Created Gold expectation suite: {suite_name}")
        
        return suite_name


def validate_bronze_data(
    df: DataFrame,
    spark: SparkSession,
    raise_on_failure: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to validate Bronze layer data.
    
    Args:
        df: Spark DataFrame to validate
        spark: SparkSession
        raise_on_failure: Whether to raise exception on validation failure
    
    Returns:
        Validation result dictionary
    """
    validator = DataQualityValidator(spark=spark)
    result = validator.validate_dataframe(df, "bronze_suite")
    
    if not result["success"] and raise_on_failure:
        raise ValueError(f"Bronze data validation failed: {json.dumps(result, indent=2)}")
    
    return result


def validate_silver_data(
    df: DataFrame,
    spark: SparkSession,
    raise_on_failure: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to validate Silver layer data.
    
    Args:
        df: Spark DataFrame to validate
        spark: SparkSession
        raise_on_failure: Whether to raise exception on validation failure
    
    Returns:
        Validation result dictionary
    """
    validator = DataQualityValidator(spark=spark)
    result = validator.validate_dataframe(df, "silver_suite")
    
    if not result["success"] and raise_on_failure:
        # Check if success rate is below threshold
        success_percent = result["statistics"]["success_percent"]
        if success_percent < 95.0:
            raise ValueError(f"Silver data validation failed: {json.dumps(result, indent=2)}")
        else:
            logger.warning(f"Silver validation had some failures but met threshold: {success_percent}%")
    
    return result


def validate_gold_data(
    df: DataFrame,
    spark: SparkSession,
    raise_on_failure: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to validate Gold layer data.
    
    Args:
        df: Spark DataFrame to validate
        spark: SparkSession
        raise_on_failure: Whether to raise exception on validation failure
    
    Returns:
        Validation result dictionary
    """
    validator = DataQualityValidator(spark=spark)
    result = validator.validate_dataframe(df, "gold_suite")
    
    if not result["success"] and raise_on_failure:
        raise ValueError(f"Gold data validation failed: {json.dumps(result, indent=2)}")
    
    return result
