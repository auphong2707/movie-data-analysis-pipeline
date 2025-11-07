"""
Great Expectations Data Quality Suite for Batch Layer

Defines validation expectations for Bronze, Silver, and Gold layers.
"""

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import DataContextError
import os


class BatchLayerExpectations:
    """
    Great Expectations suite for batch layer data quality.
    """
    
    @staticmethod
    def get_bronze_movie_expectations():
        """
        Expectations for Bronze layer movies.
        
        Returns:
            List of ExpectationConfiguration objects
        """
        return [
            # Schema expectations
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={
                    "column_list": [
                        "id", "raw_json", "api_endpoint", "extraction_timestamp",
                        "partition_year", "partition_month", "partition_day", "partition_hour"
                    ]
                }
            ),
            
            # Completeness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "raw_json"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "api_endpoint"}
            ),
            
            # Uniqueness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "id"}
            ),
            
            # Value range expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "partition_year",
                    "min_value": 2020,
                    "max_value": 2030
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "partition_month",
                    "min_value": 1,
                    "max_value": 12
                }
            ),
            
            # Row count expectations (at least some data)
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_be_between",
                kwargs={
                    "min_value": 1,
                    "max_value": 1000000
                }
            ),
        ]
    
    @staticmethod
    def get_silver_movie_expectations():
        """
        Expectations for Silver layer movies.
        
        Returns:
            List of ExpectationConfiguration objects
        """
        return [
            # Required columns
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "movie_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "title"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "release_date"}
            ),
            
            # Completeness (>95% for required fields)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "movie_id",
                    "mostly": 1.0  # 100% required
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "title",
                    "mostly": 0.95  # 95% required
                }
            ),
            
            # Data validity
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "vote_average",
                    "min_value": 0.0,
                    "max_value": 10.0,
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "vote_count",
                    "min_value": 0,
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "popularity",
                    "min_value": 0.0,
                    "mostly": 0.95
                }
            ),
            
            # Quality flags
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "quality_flag",
                    "value_set": ["OK", "WARNING", "ERROR"]
                }
            ),
            
            # Uniqueness (no duplicates after deduplication)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "movie_id"}
            ),
        ]
    
    @staticmethod
    def get_silver_review_expectations():
        """
        Expectations for Silver layer reviews.
        
        Returns:
            List of ExpectationConfiguration objects
        """
        return [
            # Required fields
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "review_id",
                    "mostly": 1.0
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "movie_id",
                    "mostly": 1.0
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "content",
                    "mostly": 0.95
                }
            ),
            
            # Sentiment score ranges
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "sentiment_score",
                    "min_value": -1.0,
                    "max_value": 1.0,
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "sentiment_label",
                    "value_set": ["positive", "negative", "neutral"]
                }
            ),
            
            # Uniqueness
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "review_id"}
            ),
        ]
    
    @staticmethod
    def get_gold_genre_analytics_expectations():
        """
        Expectations for Gold layer genre analytics.
        
        Returns:
            List of ExpectationConfiguration objects
        """
        return [
            # Required fields
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "genre"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "year"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "total_movies"}
            ),
            
            # Aggregate validity
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "total_movies",
                    "min_value": 0
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "avg_rating",
                    "min_value": 0.0,
                    "max_value": 10.0,
                    "mostly": 0.9
                }
            ),
            
            # Uniqueness (one record per genre/year/month)
            ExpectationConfiguration(
                expectation_type="expect_compound_columns_to_be_unique",
                kwargs={
                    "column_list": ["genre", "year", "month"]
                }
            ),
        ]


def run_validation(df, expectations, suite_name="default"):
    """
    Run Great Expectations validation on a DataFrame.
    
    Args:
        df: PySpark DataFrame to validate
        expectations: List of ExpectationConfiguration objects
        suite_name: Name of the expectation suite
        
    Returns:
        Validation results dictionary
    """
    try:
        from great_expectations.dataset import SparkDFDataset
        
        # Wrap DataFrame
        ge_df = SparkDFDataset(df)
        
        # Run expectations
        results = {
            "success": True,
            "results": [],
            "statistics": {
                "evaluated_expectations": 0,
                "successful_expectations": 0,
                "unsuccessful_expectations": 0,
                "success_percent": 0.0
            }
        }
        
        for expectation in expectations:
            result = ge_df.validate_expectation(expectation)
            results["results"].append(result)
            results["statistics"]["evaluated_expectations"] += 1
            
            if result.success:
                results["statistics"]["successful_expectations"] += 1
            else:
                results["statistics"]["unsuccessful_expectations"] += 1
                results["success"] = False
        
        # Calculate success percentage
        if results["statistics"]["evaluated_expectations"] > 0:
            results["statistics"]["success_percent"] = (
                results["statistics"]["successful_expectations"] / 
                results["statistics"]["evaluated_expectations"] * 100
            )
        
        return results
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "statistics": {}
        }
