"""
Data Quality Tests for TMDB Movie Data Pipeline.

This module contains comprehensive tests for data quality validation,
monitoring, and alerting across all pipeline stages.
"""

import pytest
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import json
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import pandas as pd
from dataclasses import dataclass
import yaml

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class QualityMetrics:
    """Data quality metrics container."""
    completeness: float
    consistency: float
    accuracy: float
    timeliness: float
    validity: float
    uniqueness: float


@dataclass
class QualityThresholds:
    """Quality thresholds for validation."""
    min_completeness: float = 0.95
    min_consistency: float = 0.98
    min_accuracy: float = 0.90
    min_timeliness: float = 0.95
    min_validity: float = 0.98
    min_uniqueness: float = 0.99


class DataQualityValidator:
    """Mock data quality validator for testing."""
    
    def __init__(self, thresholds: QualityThresholds):
        self.thresholds = thresholds
    
    def validate_completeness(self, data: List[Dict]) -> Tuple[float, List[str]]:
        """Validate data completeness."""
        if not data:
            return 0.0, ["No data provided"]
        
        issues = []
        total_fields = 0
        missing_fields = 0
        
        # Define required fields for different data types
        required_fields = {
            "bronze": ["movie_id", "title", "release_date"],
            "silver": ["movie_id", "title", "release_date", "genres", "rating"],
            "gold": ["movie_id", "aggregated_metrics", "computed_date"]
        }
        
        for record in data:
            # Determine data type
            data_type = self._determine_data_type(record)
            req_fields = required_fields.get(data_type, [])
            
            for field in req_fields:
                total_fields += 1
                if field not in record or record[field] is None or record[field] == "":
                    missing_fields += 1
                    if len(issues) < 10:  # Limit issue reporting
                        issues.append(f"Missing field '{field}' in record {record.get('movie_id', 'unknown')}")
        
        if total_fields == 0:
            return 1.0, []
        
        completeness_score = 1.0 - (missing_fields / total_fields)
        return completeness_score, issues
    
    def validate_consistency(self, data: List[Dict]) -> Tuple[float, List[str]]:
        """Validate data consistency."""
        if not data:
            return 1.0, []
        
        issues = []
        total_checks = 0
        consistency_violations = 0
        
        # Group data by movie_id for consistency checks
        movie_groups = {}
        for record in data:
            movie_id = record.get("movie_id")
            if movie_id:
                if movie_id not in movie_groups:
                    movie_groups[movie_id] = []
                movie_groups[movie_id].append(record)
        
        # Check consistency within movie groups
        for movie_id, records in movie_groups.items():
            if len(records) > 1:
                # Check title consistency
                titles = {r.get("title") for r in records if r.get("title")}
                total_checks += 1
                if len(titles) > 1:
                    consistency_violations += 1
                    issues.append(f"Inconsistent titles for movie {movie_id}: {titles}")
                
                # Check release date consistency
                release_dates = {r.get("release_date") for r in records if r.get("release_date")}
                total_checks += 1
                if len(release_dates) > 1:
                    consistency_violations += 1
                    issues.append(f"Inconsistent release dates for movie {movie_id}: {release_dates}")
        
        if total_checks == 0:
            return 1.0, []
        
        consistency_score = 1.0 - (consistency_violations / total_checks)
        return consistency_score, issues
    
    def validate_accuracy(self, data: List[Dict]) -> Tuple[float, List[str]]:
        """Validate data accuracy."""
        if not data:
            return 1.0, []
        
        issues = []
        total_checks = 0
        accuracy_violations = 0
        
        for record in data:
            # Check rating ranges
            if "rating" in record:
                total_checks += 1
                rating = record["rating"]
                if not isinstance(rating, (int, float)) or rating < 0 or rating > 10:
                    accuracy_violations += 1
                    issues.append(f"Invalid rating {rating} for movie {record.get('movie_id')}")
            
            # Check revenue values
            if "revenue" in record:
                total_checks += 1
                revenue = record["revenue"]
                if not isinstance(revenue, (int, float)) or revenue < 0:
                    accuracy_violations += 1
                    issues.append(f"Invalid revenue {revenue} for movie {record.get('movie_id')}")
            
            # Check date formats
            if "release_date" in record:
                total_checks += 1
                try:
                    if isinstance(record["release_date"], str):
                        datetime.strptime(record["release_date"], "%Y-%m-%d")
                except (ValueError, TypeError):
                    accuracy_violations += 1
                    issues.append(f"Invalid date format for movie {record.get('movie_id')}")
        
        if total_checks == 0:
            return 1.0, []
        
        accuracy_score = 1.0 - (accuracy_violations / total_checks)
        return accuracy_score, issues
    
    def validate_timeliness(self, data: List[Dict], max_age_hours: int = 24) -> Tuple[float, List[str]]:
        """Validate data timeliness."""
        if not data:
            return 1.0, []
        
        issues = []
        current_time = datetime.utcnow()
        max_age = timedelta(hours=max_age_hours)
        
        total_records = len(data)
        stale_records = 0
        
        for record in data:
            # Check for timestamp fields
            timestamp_fields = ["computed_at", "updated_at", "ingested_at"]
            record_timestamp = None
            
            for field in timestamp_fields:
                if field in record:
                    try:
                        if isinstance(record[field], str):
                            record_timestamp = datetime.fromisoformat(record[field].replace('Z', '+00:00'))
                        elif isinstance(record[field], datetime):
                            record_timestamp = record[field]
                        break
                    except (ValueError, TypeError):
                        continue
            
            if record_timestamp:
                age = current_time - record_timestamp
                if age > max_age:
                    stale_records += 1
                    if len(issues) < 10:
                        issues.append(f"Stale record {record.get('movie_id')}: {age} old")
        
        timeliness_score = 1.0 - (stale_records / total_records) if total_records > 0 else 1.0
        return timeliness_score, issues
    
    def validate_validity(self, data: List[Dict]) -> Tuple[float, List[str]]:
        """Validate data validity (format and type checks)."""
        if not data:
            return 1.0, []
        
        issues = []
        total_checks = 0
        validity_violations = 0
        
        for record in data:
            # Check movie_id format
            if "movie_id" in record:
                total_checks += 1
                movie_id = record["movie_id"]
                if not isinstance(movie_id, str) or len(movie_id) == 0:
                    validity_violations += 1
                    issues.append(f"Invalid movie_id format: {movie_id}")
            
            # Check genre format
            if "genres" in record:
                total_checks += 1
                genres = record["genres"]
                if isinstance(genres, list):
                    for genre in genres:
                        if not isinstance(genre, str):
                            validity_violations += 1
                            issues.append(f"Invalid genre type in movie {record.get('movie_id')}: {type(genre)}")
                            break
                else:
                    validity_violations += 1
                    issues.append(f"Genres should be list for movie {record.get('movie_id')}")
            
            # Check sentiment score format
            if "sentiment_score" in record:
                total_checks += 1
                sentiment = record["sentiment_score"]
                if not isinstance(sentiment, (int, float)) or sentiment < -1 or sentiment > 1:
                    validity_violations += 1
                    issues.append(f"Invalid sentiment score for movie {record.get('movie_id')}: {sentiment}")
        
        if total_checks == 0:
            return 1.0, []
        
        validity_score = 1.0 - (validity_violations / total_checks)
        return validity_score, issues
    
    def validate_uniqueness(self, data: List[Dict], key_field: str = "movie_id") -> Tuple[float, List[str]]:
        """Validate data uniqueness."""
        if not data:
            return 1.0, []
        
        issues = []
        values = []
        
        for record in data:
            if key_field in record:
                values.append(record[key_field])
        
        if not values:
            return 1.0, []
        
        unique_values = set(values)
        duplicates = len(values) - len(unique_values)
        
        if duplicates > 0:
            # Find specific duplicates
            value_counts = {}
            for value in values:
                value_counts[value] = value_counts.get(value, 0) + 1
            
            duplicate_values = {k: v for k, v in value_counts.items() if v > 1}
            for value, count in list(duplicate_values.items())[:10]:  # Limit reporting
                issues.append(f"Duplicate {key_field}: {value} (appears {count} times)")
        
        uniqueness_score = len(unique_values) / len(values) if values else 1.0
        return uniqueness_score, issues
    
    def _determine_data_type(self, record: Dict) -> str:
        """Determine data type based on record structure."""
        if "aggregated_metrics" in record:
            return "gold"
        elif "genres" in record and "rating" in record:
            return "silver"
        else:
            return "bronze"


class TestDataQualityValidation:
    """Test data quality validation functionality."""
    
    @pytest.fixture
    def quality_validator(self):
        """Create data quality validator."""
        thresholds = QualityThresholds()
        return DataQualityValidator(thresholds)
    
    @pytest.fixture
    def sample_bronze_data(self):
        """Create sample Bronze layer data."""
        return [
            {
                "movie_id": "bronze_001",
                "title": "Test Movie 1",
                "release_date": "2023-06-15",
                "overview": "A test movie",
                "poster_path": "/test1.jpg",
                "ingested_at": datetime.utcnow().isoformat()
            },
            {
                "movie_id": "bronze_002",
                "title": "Test Movie 2", 
                "release_date": "2023-06-16",
                "overview": "Another test movie",
                "poster_path": "/test2.jpg",
                "ingested_at": datetime.utcnow().isoformat()
            },
            {
                "movie_id": "bronze_003",
                "title": "",  # Missing title
                "release_date": "invalid-date",  # Invalid date
                "overview": None,
                "poster_path": "/test3.jpg",
                "ingested_at": (datetime.utcnow() - timedelta(hours=25)).isoformat()  # Stale
            }
        ]
    
    @pytest.fixture
    def sample_silver_data(self):
        """Create sample Silver layer data."""
        return [
            {
                "movie_id": "silver_001",
                "title": "Processed Movie 1",
                "release_date": "2023-06-15",
                "genres": ["Action", "Adventure"],
                "rating": 7.5,
                "revenue": 150000000,
                "sentiment_score": 0.65,
                "computed_at": datetime.utcnow().isoformat()
            },
            {
                "movie_id": "silver_002",
                "title": "Processed Movie 2",
                "release_date": "2023-06-16", 
                "genres": ["Drama", "Romance"],
                "rating": 8.2,
                "revenue": 85000000,
                "sentiment_score": 0.78,
                "computed_at": datetime.utcnow().isoformat()
            },
            {
                "movie_id": "silver_003",
                "title": "Processed Movie 3",
                "release_date": "2023-06-17",
                "genres": "Action",  # Should be list
                "rating": 15.0,  # Invalid rating
                "revenue": -1000000,  # Invalid revenue
                "sentiment_score": 2.0,  # Invalid sentiment
                "computed_at": datetime.utcnow().isoformat()
            }
        ]
    
    def test_completeness_validation(self, quality_validator, sample_bronze_data):
        """Test completeness validation."""
        logger.info("Testing completeness validation")
        
        score, issues = quality_validator.validate_completeness(sample_bronze_data)
        
        # Should detect missing title in third record
        assert score < 1.0
        assert len(issues) > 0
        assert any("Missing field" in issue for issue in issues)
        
        logger.info(f"Completeness score: {score}, Issues: {len(issues)}")
    
    def test_consistency_validation(self, quality_validator):
        """Test consistency validation.""" 
        logger.info("Testing consistency validation")
        
        # Create data with consistency issues
        inconsistent_data = [
            {"movie_id": "movie_001", "title": "Original Title", "release_date": "2023-01-01"},
            {"movie_id": "movie_001", "title": "Different Title", "release_date": "2023-01-01"},  # Inconsistent title
            {"movie_id": "movie_002", "title": "Consistent Movie", "release_date": "2023-01-02"}
        ]
        
        score, issues = quality_validator.validate_consistency(inconsistent_data)
        
        # Should detect title inconsistency
        assert score < 1.0
        assert len(issues) > 0
        assert any("Inconsistent titles" in issue for issue in issues)
        
        logger.info(f"Consistency score: {score}, Issues: {len(issues)}")
    
    def test_accuracy_validation(self, quality_validator, sample_silver_data):
        """Test accuracy validation."""
        logger.info("Testing accuracy validation")
        
        score, issues = quality_validator.validate_accuracy(sample_silver_data)
        
        # Should detect invalid rating, revenue, and sentiment in third record
        assert score < 1.0
        assert len(issues) > 0
        assert any("Invalid rating" in issue for issue in issues)
        assert any("Invalid revenue" in issue for issue in issues)
        
        logger.info(f"Accuracy score: {score}, Issues: {len(issues)}")
    
    def test_timeliness_validation(self, quality_validator, sample_bronze_data):
        """Test timeliness validation."""
        logger.info("Testing timeliness validation")
        
        score, issues = quality_validator.validate_timeliness(sample_bronze_data, max_age_hours=24)
        
        # Should detect stale record (25 hours old)
        assert score < 1.0
        assert len(issues) > 0
        assert any("Stale record" in issue for issue in issues)
        
        logger.info(f"Timeliness score: {score}, Issues: {len(issues)}")
    
    def test_validity_validation(self, quality_validator, sample_silver_data):
        """Test validity validation."""
        logger.info("Testing validity validation")
        
        score, issues = quality_validator.validate_validity(sample_silver_data)
        
        # Should detect format issues in third record
        assert score < 1.0
        assert len(issues) > 0
        assert any("should be list" in issue or "Invalid sentiment" in issue for issue in issues)
        
        logger.info(f"Validity score: {score}, Issues: {len(issues)}")
    
    def test_uniqueness_validation(self, quality_validator):
        """Test uniqueness validation."""
        logger.info("Testing uniqueness validation")
        
        # Create data with duplicates
        duplicate_data = [
            {"movie_id": "movie_001", "title": "Movie 1"},
            {"movie_id": "movie_002", "title": "Movie 2"},
            {"movie_id": "movie_001", "title": "Movie 1 Duplicate"},  # Duplicate ID
            {"movie_id": "movie_003", "title": "Movie 3"}
        ]
        
        score, issues = quality_validator.validate_uniqueness(duplicate_data, "movie_id")
        
        # Should detect duplicate movie_id
        assert score < 1.0
        assert len(issues) > 0
        assert any("Duplicate movie_id" in issue for issue in issues)
        
        logger.info(f"Uniqueness score: {score}, Issues: {len(issues)}")


class TestQualityThresholds:
    """Test quality threshold validation and alerting."""
    
    @pytest.fixture
    def quality_thresholds(self):
        """Create quality thresholds."""
        return QualityThresholds(
            min_completeness=0.95,
            min_consistency=0.98,
            min_accuracy=0.90,
            min_timeliness=0.95,
            min_validity=0.98,
            min_uniqueness=0.99
        )
    
    def test_threshold_validation(self, quality_thresholds):
        """Test quality threshold validation."""
        logger.info("Testing quality threshold validation")
        
        # Test passing scores
        passing_metrics = QualityMetrics(
            completeness=0.98,
            consistency=0.99,
            accuracy=0.95,
            timeliness=0.97,
            validity=0.99,
            uniqueness=1.0
        )
        
        violations = self._check_quality_violations(passing_metrics, quality_thresholds)
        assert len(violations) == 0
        
        # Test failing scores
        failing_metrics = QualityMetrics(
            completeness=0.90,  # Below threshold
            consistency=0.95,   # Below threshold
            accuracy=0.85,      # Below threshold
            timeliness=0.90,    # Below threshold
            validity=0.95,      # Below threshold
            uniqueness=0.95     # Below threshold
        )
        
        violations = self._check_quality_violations(failing_metrics, quality_thresholds)
        assert len(violations) == 6  # All metrics fail
        
        logger.info(f"Threshold validation test passed: {len(violations)} violations detected")
    
    def _check_quality_violations(self, metrics: QualityMetrics, thresholds: QualityThresholds) -> List[str]:
        """Check for quality violations against thresholds."""
        violations = []
        
        if metrics.completeness < thresholds.min_completeness:
            violations.append(f"Completeness {metrics.completeness} < {thresholds.min_completeness}")
        
        if metrics.consistency < thresholds.min_consistency:
            violations.append(f"Consistency {metrics.consistency} < {thresholds.min_consistency}")
        
        if metrics.accuracy < thresholds.min_accuracy:
            violations.append(f"Accuracy {metrics.accuracy} < {thresholds.min_accuracy}")
        
        if metrics.timeliness < thresholds.min_timeliness:
            violations.append(f"Timeliness {metrics.timeliness} < {thresholds.min_timeliness}")
        
        if metrics.validity < thresholds.min_validity:
            violations.append(f"Validity {metrics.validity} < {thresholds.min_validity}")
        
        if metrics.uniqueness < thresholds.min_uniqueness:
            violations.append(f"Uniqueness {metrics.uniqueness} < {thresholds.min_uniqueness}")
        
        return violations


class TestQualityMonitoring:
    """Test quality monitoring and alerting."""
    
    def test_quality_report_generation(self):
        """Test quality report generation."""
        logger.info("Testing quality report generation")
        
        # Simulate quality metrics from pipeline
        quality_metrics = {
            "bronze_layer": QualityMetrics(0.98, 0.99, 0.95, 0.97, 0.99, 1.0),
            "silver_layer": QualityMetrics(0.96, 0.98, 0.93, 0.96, 0.98, 0.99),
            "gold_layer": QualityMetrics(0.99, 0.99, 0.98, 0.99, 0.99, 1.0)
        }
        
        # Generate quality report
        report = self._generate_quality_report(quality_metrics)
        
        # Validate report structure
        assert "summary" in report
        assert "layer_details" in report
        assert "violations" in report
        assert "timestamp" in report
        
        # Check layer details
        for layer in ["bronze_layer", "silver_layer", "gold_layer"]:
            assert layer in report["layer_details"]
            layer_report = report["layer_details"][layer]
            assert "overall_score" in layer_report
            assert "metrics" in layer_report
        
        logger.info("Quality report generation test passed")
    
    def test_alerting_triggers(self):
        """Test quality alerting triggers."""
        logger.info("Testing quality alerting triggers")
        
        # Create metrics that should trigger alerts
        critical_metrics = QualityMetrics(
            completeness=0.85,  # Critical threshold
            consistency=0.90,   # Warning threshold
            accuracy=0.88,      # Critical threshold
            timeliness=0.92,    # Warning threshold
            validity=0.87,      # Critical threshold
            uniqueness=0.96     # Warning threshold
        )
        
        alerts = self._generate_quality_alerts(critical_metrics)
        
        # Should have both critical and warning alerts
        critical_alerts = [a for a in alerts if a["severity"] == "critical"]
        warning_alerts = [a for a in alerts if a["severity"] == "warning"]
        
        assert len(critical_alerts) > 0
        assert len(warning_alerts) > 0
        
        # Check alert structure
        for alert in alerts:
            assert "metric" in alert
            assert "severity" in alert
            assert "message" in alert
            assert "timestamp" in alert
        
        logger.info(f"Alerting test passed: {len(critical_alerts)} critical, {len(warning_alerts)} warning")
    
    def _generate_quality_report(self, quality_metrics: Dict[str, QualityMetrics]) -> Dict:
        """Generate quality report."""
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {},
            "layer_details": {},
            "violations": []
        }
        
        all_scores = []
        thresholds = QualityThresholds()
        
        for layer, metrics in quality_metrics.items():
            # Calculate overall score for layer
            layer_scores = [
                metrics.completeness,
                metrics.consistency,
                metrics.accuracy,
                metrics.timeliness,
                metrics.validity,
                metrics.uniqueness
            ]
            overall_score = sum(layer_scores) / len(layer_scores)
            all_scores.append(overall_score)
            
            # Add layer details
            report["layer_details"][layer] = {
                "overall_score": overall_score,
                "metrics": {
                    "completeness": metrics.completeness,
                    "consistency": metrics.consistency,
                    "accuracy": metrics.accuracy,
                    "timeliness": metrics.timeliness,
                    "validity": metrics.validity,
                    "uniqueness": metrics.uniqueness
                }
            }
            
            # Check for violations
            violations = self._check_layer_violations(metrics, thresholds, layer)
            report["violations"].extend(violations)
        
        # Overall summary
        report["summary"] = {
            "overall_score": sum(all_scores) / len(all_scores),
            "layers_passing": sum(1 for score in all_scores if score >= 0.90),
            "total_violations": len(report["violations"])
        }
        
        return report
    
    def _generate_quality_alerts(self, metrics: QualityMetrics) -> List[Dict]:
        """Generate quality alerts based on metrics."""
        alerts = []
        timestamp = datetime.utcnow().isoformat()
        
        # Define alert thresholds
        critical_threshold = 0.90
        warning_threshold = 0.95
        
        metric_checks = [
            ("completeness", metrics.completeness),
            ("consistency", metrics.consistency),
            ("accuracy", metrics.accuracy),
            ("timeliness", metrics.timeliness),
            ("validity", metrics.validity),
            ("uniqueness", metrics.uniqueness)
        ]
        
        for metric_name, value in metric_checks:
            if value < critical_threshold:
                alerts.append({
                    "metric": metric_name,
                    "severity": "critical",
                    "message": f"{metric_name.title()} critically low: {value:.3f}",
                    "timestamp": timestamp
                })
            elif value < warning_threshold:
                alerts.append({
                    "metric": metric_name,
                    "severity": "warning",
                    "message": f"{metric_name.title()} below optimal: {value:.3f}",
                    "timestamp": timestamp
                })
        
        return alerts
    
    def _check_layer_violations(self, metrics: QualityMetrics, thresholds: QualityThresholds, layer: str) -> List[str]:
        """Check for quality violations in a layer."""
        violations = []
        
        checks = [
            ("completeness", metrics.completeness, thresholds.min_completeness),
            ("consistency", metrics.consistency, thresholds.min_consistency),
            ("accuracy", metrics.accuracy, thresholds.min_accuracy),
            ("timeliness", metrics.timeliness, thresholds.min_timeliness),
            ("validity", metrics.validity, thresholds.min_validity),
            ("uniqueness", metrics.uniqueness, thresholds.min_uniqueness)
        ]
        
        for metric_name, actual, threshold in checks:
            if actual < threshold:
                violations.append(f"{layer}: {metric_name} {actual:.3f} < {threshold:.3f}")
        
        return violations


class TestDataLineage:
    """Test data lineage tracking for quality audit trails."""
    
    @pytest.fixture
    def sample_lineage_data(self):
        """Create sample data lineage information."""
        return {
            "movie_001": {
                "bronze_ingestion": {
                    "source": "tmdb_api",
                    "timestamp": "2023-06-15T10:00:00Z",
                    "batch_id": "batch_20230615_100000",
                    "quality_score": 0.95
                },
                "silver_transformation": {
                    "source": "bronze_movie_001",
                    "timestamp": "2023-06-15T12:00:00Z",
                    "batch_id": "batch_20230615_120000",
                    "quality_score": 0.98,
                    "transformations": ["genre_parsing", "sentiment_analysis"]
                },
                "gold_aggregation": {
                    "source": "silver_movie_001",
                    "timestamp": "2023-06-15T14:00:00Z",
                    "batch_id": "batch_20230615_140000",
                    "quality_score": 0.99,
                    "aggregations": ["trending_score", "genre_metrics"]
                }
            }
        }
    
    def test_lineage_tracking(self, sample_lineage_data):
        """Test data lineage tracking."""
        logger.info("Testing data lineage tracking")
        
        movie_lineage = sample_lineage_data["movie_001"]
        
        # Validate lineage stages
        expected_stages = ["bronze_ingestion", "silver_transformation", "gold_aggregation"]
        for stage in expected_stages:
            assert stage in movie_lineage
            stage_info = movie_lineage[stage]
            
            # Validate stage structure
            assert "source" in stage_info
            assert "timestamp" in stage_info
            assert "batch_id" in stage_info
            assert "quality_score" in stage_info
            
            # Validate quality progression (should generally improve)
            assert 0.0 <= stage_info["quality_score"] <= 1.0
        
        # Check quality progression
        bronze_quality = movie_lineage["bronze_ingestion"]["quality_score"]
        silver_quality = movie_lineage["silver_transformation"]["quality_score"]
        gold_quality = movie_lineage["gold_aggregation"]["quality_score"]
        
        # Quality should generally improve through pipeline
        assert silver_quality >= bronze_quality
        assert gold_quality >= silver_quality
        
        logger.info("Data lineage tracking test passed")
    
    def test_lineage_quality_correlation(self, sample_lineage_data):
        """Test correlation between lineage and quality."""
        logger.info("Testing lineage quality correlation")
        
        # Extract quality scores across pipeline stages
        quality_progression = []
        
        for movie_id, lineage in sample_lineage_data.items():
            progression = []
            for stage in ["bronze_ingestion", "silver_transformation", "gold_aggregation"]:
                if stage in lineage:
                    progression.append(lineage[stage]["quality_score"])
            quality_progression.append(progression)
        
        # Analyze quality trends
        for progression in quality_progression:
            if len(progression) >= 2:
                # Check for overall quality maintenance/improvement
                initial_quality = progression[0]
                final_quality = progression[-1]
                
                # Final quality should not be significantly worse than initial
                quality_degradation = initial_quality - final_quality
                assert quality_degradation <= 0.1  # Allow small degradation
        
        logger.info("Lineage quality correlation test passed")


class TestIntegrationQuality:
    """Integration tests for end-to-end quality validation."""
    
    def test_pipeline_quality_integration(self):
        """Test complete pipeline quality validation."""
        logger.info("Testing pipeline quality integration")
        
        # Simulate complete pipeline run with quality tracking
        pipeline_run = {
            "batch_id": "integration_test_batch",
            "start_time": datetime.utcnow(),
            "stages": []
        }
        
        # Bronze stage
        bronze_stage = self._simulate_stage_execution(
            "bronze_ingestion",
            input_count=1000,
            expected_output_count=980,  # Some data loss
            quality_score=0.96
        )
        pipeline_run["stages"].append(bronze_stage)
        
        # Silver stage
        silver_stage = self._simulate_stage_execution(
            "silver_transformation", 
            input_count=980,
            expected_output_count=975,  # Minimal loss
            quality_score=0.98
        )
        pipeline_run["stages"].append(silver_stage)
        
        # Gold stage
        gold_stage = self._simulate_stage_execution(
            "gold_aggregation",
            input_count=975,
            expected_output_count=970,  # Aggregation reduces count
            quality_score=0.99
        )
        pipeline_run["stages"].append(gold_stage)
        
        # Validate pipeline quality
        pipeline_quality = self._validate_pipeline_quality(pipeline_run)
        
        assert pipeline_quality["overall_score"] >= 0.95
        assert pipeline_quality["data_loss_ratio"] <= 0.05
        assert len(pipeline_quality["quality_issues"]) <= 2
        
        logger.info(f"Pipeline quality integration test passed: {pipeline_quality['overall_score']:.3f}")
    
    def _simulate_stage_execution(self, stage_name: str, input_count: int, 
                                 expected_output_count: int, quality_score: float) -> Dict:
        """Simulate pipeline stage execution."""
        return {
            "stage_name": stage_name,
            "input_count": input_count,
            "output_count": expected_output_count,
            "quality_score": quality_score,
            "execution_time": timedelta(minutes=15),
            "errors": [],
            "warnings": []
        }
    
    def _validate_pipeline_quality(self, pipeline_run: Dict) -> Dict:
        """Validate overall pipeline quality."""
        stages = pipeline_run["stages"]
        
        # Calculate overall metrics
        quality_scores = [stage["quality_score"] for stage in stages]
        overall_score = sum(quality_scores) / len(quality_scores)
        
        # Calculate data loss
        initial_count = stages[0]["input_count"]
        final_count = stages[-1]["output_count"]
        data_loss_ratio = (initial_count - final_count) / initial_count
        
        # Collect quality issues
        quality_issues = []
        for stage in stages:
            if stage["quality_score"] < 0.95:
                quality_issues.append(f"{stage['stage_name']}: Low quality {stage['quality_score']}")
            
            if len(stage["errors"]) > 0:
                quality_issues.append(f"{stage['stage_name']}: {len(stage['errors'])} errors")
        
        return {
            "overall_score": overall_score,
            "data_loss_ratio": data_loss_ratio,
            "quality_issues": quality_issues,
            "stage_count": len(stages)
        }


if __name__ == "__main__":
    pytest.main([__file__, "-v"])