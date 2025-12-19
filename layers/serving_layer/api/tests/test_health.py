"""
Integration Tests for Health Check Endpoints
Tests the health monitoring system for Lambda Architecture components
"""
import pytest
from fastapi.testclient import TestClient
from datetime import datetime
import json
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from api.main import app
from api.routes.health import HealthChecker

client = TestClient(app)


class TestBasicHealthEndpoint:
    """Test basic health check endpoint"""
    
    def test_basic_health_check_returns_200(self):
        """Test that basic health check returns 200 status"""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
    
    def test_basic_health_check_structure(self):
        """Test that basic health check returns expected structure"""
        response = client.get("/api/v1/health")
        data = response.json()
        
        assert "status" in data
        assert "timestamp" in data
        assert "service" in data
        assert "version" in data
        
        assert data["status"] == "healthy"
        assert data["service"] == "serving-api"
        assert data["version"] == "2.0.0"
    
    def test_basic_health_check_timestamp_format(self):
        """Test that timestamp is in ISO format"""
        response = client.get("/api/v1/health")
        data = response.json()
        
        # Should be able to parse as ISO format
        timestamp = datetime.fromisoformat(data["timestamp"].replace('Z', '+00:00'))
        assert isinstance(timestamp, datetime)


class TestDetailedHealthEndpoint:
    """Test detailed health check endpoint"""
    
    def test_detailed_health_check_returns_200(self):
        """Test that detailed health check returns 200 status"""
        response = client.get("/api/v1/health/detailed")
        assert response.status_code == 200
    
    def test_detailed_health_check_structure(self):
        """Test that detailed health check returns expected structure"""
        response = client.get("/api/v1/health/detailed")
        data = response.json()
        
        assert "status" in data
        assert "timestamp" in data
        assert "components" in data
        
        components = data["components"]
        assert "mongodb" in components
        assert "redis" in components
        assert "api" in components
    
    def test_detailed_health_check_component_status(self):
        """Test that each component has a status field"""
        response = client.get("/api/v1/health/detailed")
        data = response.json()
        
        for component_name, component_data in data["components"].items():
            assert "status" in component_data
            assert component_data["status"] in ["healthy", "degraded", "unhealthy"]
    
    def test_detailed_health_mongodb_metrics(self):
        """Test MongoDB component returns expected metrics when healthy"""
        response = client.get("/api/v1/health/detailed")
        data = response.json()
        mongodb = data["components"]["mongodb"]
        
        if mongodb["status"] == "healthy":
            assert "response_time_ms" in mongodb
            assert "collections_count" in mongodb
            assert "data_size_mb" in mongodb
            assert "storage_size_mb" in mongodb
            
            assert isinstance(mongodb["response_time_ms"], (int, float))
            assert isinstance(mongodb["collections_count"], int)
            assert isinstance(mongodb["data_size_mb"], (int, float))
            assert isinstance(mongodb["storage_size_mb"], (int, float))
    
    def test_detailed_health_redis_metrics(self):
        """Test Redis component returns expected metrics when healthy"""
        response = client.get("/api/v1/health/detailed")
        data = response.json()
        redis = data["components"]["redis"]
        
        if redis["status"] == "healthy":
            assert "response_time_ms" in redis
            assert "used_memory_mb" in redis
            assert "connected_clients" in redis
            assert "uptime_hours" in redis
            
            assert isinstance(redis["response_time_ms"], (int, float))
            assert isinstance(redis["used_memory_mb"], (int, float))
            assert isinstance(redis["connected_clients"], int)
            assert isinstance(redis["uptime_hours"], (int, float))
    
    def test_detailed_health_overall_status_logic(self):
        """Test that overall status reflects component statuses correctly"""
        response = client.get("/api/v1/health/detailed")
        data = response.json()
        
        components = data["components"]
        component_statuses = [comp["status"] for comp in components.values()]
        
        if any(status == "unhealthy" for status in component_statuses):
            assert data["status"] == "unhealthy"
        elif any(status == "degraded" for status in component_statuses):
            assert data["status"] == "degraded"
        else:
            assert data["status"] == "healthy"


class TestSystemHealthEndpoint:
    """Test system-wide health check endpoint"""
    
    def test_system_health_check_returns_200(self):
        """Test that system health check returns 200 status"""
        response = client.get("/api/v1/health/system")
        assert response.status_code == 200
    
    def test_system_health_check_structure(self):
        """Test that system health check returns expected structure"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        assert "status" in data
        assert "timestamp" in data
        assert "layers" in data
        assert "summary" in data
        
        layers = data["layers"]
        assert "serving" in layers
        assert "batch" in layers
        assert "speed" in layers
    
    def test_system_health_serving_layer_services(self):
        """Test that serving layer contains expected services"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        serving = data["layers"]["serving"]
        assert "status" in serving
        assert "services" in serving
        assert "total_services" in serving
        assert "healthy_count" in serving
        
        services = serving["services"]
        expected_services = ["mongodb", "redis", "prometheus", "grafana", "mongo_express", "api"]
        
        for service in expected_services:
            assert service in services
            assert "status" in services[service]
    
    def test_system_health_batch_layer_services(self):
        """Test that batch layer contains expected services"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        batch = data["layers"]["batch"]
        services = batch["services"]
        expected_services = ["minio", "postgres", "airflow_webserver", "airflow_scheduler", "pyspark_runner"]
        
        for service in expected_services:
            assert service in services
            assert "status" in services[service]
    
    def test_system_health_speed_layer_services(self):
        """Test that speed layer contains expected services"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        speed = data["layers"]["speed"]
        services = speed["services"]
        expected_services = [
            "zookeeper", "kafka_broker_1", "kafka_broker_2", "kafka_broker_3",
            "schema_registry", "cassandra", "reddit_producer", "sentiment_stream", "cassandra_sync"
        ]
        
        for service in expected_services:
            assert service in services
            assert "status" in services[service]
    
    def test_system_health_summary_structure(self):
        """Test that summary contains expected fields"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        summary = data["summary"]
        assert "total_services" in summary
        assert "healthy" in summary
        assert "degraded" in summary
        assert "unknown" in summary
        assert "unhealthy" in summary
        
        assert summary["total_services"] == 20
        
        # All counts should be non-negative integers
        assert isinstance(summary["healthy"], int) and summary["healthy"] >= 0
        assert isinstance(summary["degraded"], int) and summary["degraded"] >= 0
        assert isinstance(summary["unknown"], int) and summary["unknown"] >= 0
        assert isinstance(summary["unhealthy"], int) and summary["unhealthy"] >= 0
    
    def test_system_health_service_count_accuracy(self):
        """Test that service counts add up correctly"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        summary = data["summary"]
        total_counted = summary["healthy"] + summary["degraded"] + summary["unknown"] + summary["unhealthy"]
        
        assert total_counted == summary["total_services"]
        assert total_counted == 20
    
    def test_system_health_layer_counts(self):
        """Test that each layer reports correct service counts"""
        response = client.get("/api/v1/health/system")
        data = response.json()
        
        layers = data["layers"]
        
        # Serving layer should have 6 services
        assert layers["serving"]["total_services"] == 6
        
        # Batch layer should have 5 services
        assert layers["batch"]["total_services"] == 5
        
        # Speed layer should have 9 services (including 3 kafka brokers)
        assert layers["speed"]["total_services"] == 9


class TestMetricsEndpoint:
    """Test system metrics endpoint"""
    
    def test_metrics_endpoint_returns_200(self):
        """Test that metrics endpoint returns 200 status"""
        response = client.get("/api/v1/health/metrics")
        assert response.status_code == 200
    
    def test_metrics_endpoint_structure(self):
        """Test that metrics endpoint returns expected structure"""
        response = client.get("/api/v1/health/metrics")
        data = response.json()
        
        assert "timestamp" in data
        assert "metrics" in data
        
        metrics = data["metrics"]
        assert "mongodb" in metrics
        assert "redis" in metrics
    
    def test_metrics_mongodb_fields(self):
        """Test that MongoDB metrics contain expected fields"""
        response = client.get("/api/v1/health/metrics")
        data = response.json()
        
        mongodb = data["metrics"]["mongodb"]
        expected_fields = ["collections_count", "data_size_mb", "storage_size_mb", "response_time_ms"]
        
        for field in expected_fields:
            assert field in mongodb
            assert isinstance(mongodb[field], (int, float))
            assert mongodb[field] >= 0
    
    def test_metrics_redis_fields(self):
        """Test that Redis metrics contain expected fields"""
        response = client.get("/api/v1/health/metrics")
        data = response.json()
        
        redis = data["metrics"]["redis"]
        expected_fields = ["used_memory_mb", "connected_clients", "uptime_hours", "response_time_ms"]
        
        for field in expected_fields:
            assert field in redis
            assert isinstance(redis[field], (int, float))
            assert redis[field] >= 0


class TestHealthCheckerUnit:
    """Unit tests for HealthChecker class methods"""
    
    @pytest.mark.asyncio
    async def test_mongodb_checker_returns_dict(self):
        """Test that MongoDB checker returns a dictionary"""
        result = await HealthChecker.check_mongodb()
        assert isinstance(result, dict)
        assert "status" in result
    
    @pytest.mark.asyncio
    async def test_redis_checker_returns_dict(self):
        """Test that Redis checker returns a dictionary"""
        result = await HealthChecker.check_redis()
        assert isinstance(result, dict)
        assert "status" in result
    
    @pytest.mark.asyncio
    async def test_http_checker_handles_invalid_url(self):
        """Test that HTTP checker handles invalid URLs gracefully"""
        result = await HealthChecker.check_http_service("http://nonexistent-service:9999", timeout=2)
        assert result["status"] == "unhealthy"
        assert "error" in result
    
    @pytest.mark.asyncio
    async def test_mongodb_checker_handles_bad_uri(self):
        """Test that MongoDB checker handles bad URIs gracefully"""
        result = await HealthChecker.check_mongodb("mongodb://invalid:27017")
        assert result["status"] == "unhealthy"
        assert "error" in result
    
    @pytest.mark.asyncio
    async def test_redis_checker_handles_bad_host(self):
        """Test that Redis checker handles bad hosts gracefully"""
        result = await HealthChecker.check_redis("nonexistent-redis", 6379)
        assert result["status"] == "unhealthy"
        assert "error" in result


class TestHealthEndpointsIntegration:
    """Integration tests verifying end-to-end functionality"""
    
    def test_all_health_endpoints_accessible(self):
        """Test that all health endpoints are accessible"""
        endpoints = [
            "/api/v1/health",
            "/api/v1/health/detailed",
            "/api/v1/health/system",
            "/api/v1/health/metrics"
        ]
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.status_code == 200, f"Endpoint {endpoint} failed with status {response.status_code}"
    
    def test_health_endpoints_return_json(self):
        """Test that all health endpoints return valid JSON"""
        endpoints = [
            "/api/v1/health",
            "/api/v1/health/detailed",
            "/api/v1/health/system",
            "/api/v1/health/metrics"
        ]
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.headers["content-type"] == "application/json"
            # Should not raise exception
            data = response.json()
            assert isinstance(data, dict)
    
    def test_health_endpoints_timestamps_recent(self):
        """Test that timestamps in responses are recent"""
        endpoints = [
            "/api/v1/health",
            "/api/v1/health/detailed",
            "/api/v1/health/system",
            "/api/v1/health/metrics"
        ]
        
        now = datetime.utcnow()
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            data = response.json()
            
            if "timestamp" in data:
                timestamp = datetime.fromisoformat(data["timestamp"].replace('Z', '+00:00'))
                time_diff = abs((now - timestamp.replace(tzinfo=None)).total_seconds())
                
                # Timestamp should be within last 5 seconds
                assert time_diff < 5, f"Timestamp too old for {endpoint}: {time_diff}s"
    
    def test_consistent_service_status_across_endpoints(self):
        """Test that service status is consistent between detailed and system endpoints"""
        detailed_response = client.get("/api/v1/health/detailed")
        system_response = client.get("/api/v1/health/system")
        
        detailed_data = detailed_response.json()
        system_data = system_response.json()
        
        # MongoDB status should match
        detailed_mongodb = detailed_data["components"]["mongodb"]["status"]
        system_mongodb = system_data["layers"]["serving"]["services"]["mongodb"]["status"]
        assert detailed_mongodb == system_mongodb
        
        # Redis status should match
        detailed_redis = detailed_data["components"]["redis"]["status"]
        system_redis = system_data["layers"]["serving"]["services"]["redis"]["status"]
        assert detailed_redis == system_redis
    
    def test_metrics_consistency_with_detailed_check(self):
        """Test that metrics values are consistent with detailed health check"""
        detailed_response = client.get("/api/v1/health/detailed")
        metrics_response = client.get("/api/v1/health/metrics")
        
        detailed_data = detailed_response.json()
        metrics_data = metrics_response.json()
        
        # If MongoDB is healthy in detailed, metrics should have valid data
        if detailed_data["components"]["mongodb"]["status"] == "healthy":
            assert metrics_data["metrics"]["mongodb"]["collections_count"] > 0
            assert metrics_data["metrics"]["mongodb"]["response_time_ms"] > 0
        
        # If Redis is healthy in detailed, metrics should have valid data
        if detailed_data["components"]["redis"]["status"] == "healthy":
            assert metrics_data["metrics"]["redis"]["response_time_ms"] > 0


class TestHealthEndpointPerformance:
    """Performance tests for health endpoints"""
    
    def test_basic_health_check_fast_response(self):
        """Test that basic health check responds quickly"""
        import time
        start = time.time()
        response = client.get("/api/v1/health")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        assert elapsed < 1.0, f"Basic health check took too long: {elapsed}s"
    
    def test_detailed_health_check_reasonable_time(self):
        """Test that detailed health check completes in reasonable time"""
        import time
        start = time.time()
        response = client.get("/api/v1/health/detailed")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        # Should complete within 10 seconds even with slow DB
        assert elapsed < 10.0, f"Detailed health check took too long: {elapsed}s"
    
    def test_system_health_check_reasonable_time(self):
        """Test that system health check completes in reasonable time"""
        import time
        start = time.time()
        response = client.get("/api/v1/health/system")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        # Should complete within 15 seconds even with multiple services
        assert elapsed < 15.0, f"System health check took too long: {elapsed}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
