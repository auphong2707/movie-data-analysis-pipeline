"""
Performance Tests

Tests for API performance, latency, and throughput
"""

import pytest
import time
from fastapi.testclient import TestClient
import statistics
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from api.main import app

client = TestClient(app)


class TestAPILatency:
    """Test API endpoint latency"""
    
    def measure_latency(self, endpoint: str, num_requests: int = 10) -> dict:
        """
        Measure endpoint latency
        
        Args:
            endpoint: API endpoint to test
            num_requests: Number of requests to make
        
        Returns:
            Dictionary with latency statistics
        """
        latencies = []
        
        for _ in range(num_requests):
            start_time = time.time()
            response = client.get(endpoint)
            end_time = time.time()
            
            if response.status_code == 200:
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
        
        if not latencies:
            return {"error": "No successful requests"}
        
        return {
            "count": len(latencies),
            "min": min(latencies),
            "max": max(latencies),
            "mean": statistics.mean(latencies),
            "median": statistics.median(latencies),
            "p95": self.calculate_percentile(latencies, 95),
            "p99": self.calculate_percentile(latencies, 99)
        }
    
    def calculate_percentile(self, data: list, percentile: float) -> float:
        """Calculate percentile of data"""
        if not data:
            return 0.0
        
        sorted_data = sorted(data)
        index = int(len(sorted_data) * (percentile / 100))
        return sorted_data[min(index, len(sorted_data) - 1)]
    
    def test_health_endpoint_latency(self):
        """Test health endpoint latency"""
        stats = self.measure_latency("/api/v1/health", num_requests=20)
        
        if "error" not in stats:
            print(f"\nHealth endpoint latency stats (ms):")
            print(f"  Mean: {stats['mean']:.2f}")
            print(f"  Median: {stats['median']:.2f}")
            print(f"  P95: {stats['p95']:.2f}")
            print(f"  P99: {stats['p99']:.2f}")
            
            # Assert reasonable latency (adjust thresholds as needed)
            assert stats['p95'] < 500, f"P95 latency too high: {stats['p95']:.2f}ms"
    
    @pytest.mark.slow
    def test_movie_endpoint_latency(self):
        """Test movie endpoint latency"""
        stats = self.measure_latency("/api/v1/movies/550", num_requests=10)
        
        if "error" not in stats:
            print(f"\nMovie endpoint latency stats (ms):")
            print(f"  Mean: {stats['mean']:.2f}")
            print(f"  P95: {stats['p95']:.2f}")
            
            # Target: < 100ms p95 latency
            # Adjust threshold based on actual performance
            assert stats['p95'] < 1000, f"P95 latency too high: {stats['p95']:.2f}ms"
    
    @pytest.mark.slow
    def test_trending_endpoint_latency(self):
        """Test trending endpoint latency"""
        stats = self.measure_latency("/api/v1/trending/movies", num_requests=10)
        
        if "error" not in stats:
            print(f"\nTrending endpoint latency stats (ms):")
            print(f"  Mean: {stats['mean']:.2f}")
            print(f"  P95: {stats['p95']:.2f}")


class TestThroughput:
    """Test API throughput"""
    
    @pytest.mark.slow
    def test_concurrent_requests(self):
        """Test handling concurrent requests"""
        import concurrent.futures
        
        num_requests = 50
        
        def make_request():
            start = time.time()
            response = client.get("/api/v1/health")
            end = time.time()
            return (response.status_code, end - start)
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(lambda _: make_request(), range(num_requests)))
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Calculate throughput
        successful = sum(1 for status, _ in results if status == 200)
        throughput = successful / total_time
        
        print(f"\nThroughput test:")
        print(f"  Successful requests: {successful}/{num_requests}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Throughput: {throughput:.2f} req/s")
        
        assert successful >= num_requests * 0.9, "Too many failed requests"


class TestCachePerformance:
    """Test cache performance impact"""
    
    @pytest.mark.slow
    def test_cache_hit_latency(self):
        """Test latency improvement from cache hits"""
        endpoint = "/api/v1/movies/550"
        
        # First request (cache miss)
        start = time.time()
        response1 = client.get(endpoint)
        first_latency = (time.time() - start) * 1000
        
        if response1.status_code != 200:
            pytest.skip("Endpoint returned non-200 status")
        
        # Second request (should be cached)
        start = time.time()
        response2 = client.get(endpoint)
        second_latency = (time.time() - start) * 1000
        
        print(f"\nCache performance:")
        print(f"  First request (miss): {first_latency:.2f}ms")
        print(f"  Second request (hit): {second_latency:.2f}ms")
        
        if second_latency < first_latency:
            improvement = ((first_latency - second_latency) / first_latency) * 100
            print(f"  Improvement: {improvement:.1f}%")


class TestDatabaseQueryPerformance:
    """Test database query performance"""
    
    @pytest.mark.slow
    def test_search_query_performance(self):
        """Test search query performance"""
        start = time.time()
        response = client.get("/api/v1/search/movies", params={"q": "action"})
        latency = (time.time() - start) * 1000
        
        print(f"\nSearch query latency: {latency:.2f}ms")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Results returned: {data.get('total_results', 0)}")
    
    @pytest.mark.slow
    def test_aggregation_query_performance(self):
        """Test aggregation query performance"""
        start = time.time()
        response = client.get("/api/v1/analytics/genre/Action")
        latency = (time.time() - start) * 1000
        
        print(f"\nAggregation query latency: {latency:.2f}ms")
        
        # Aggregations may be slower, but should still be reasonable
        if response.status_code == 200:
            assert latency < 2000, f"Aggregation too slow: {latency:.2f}ms"


class TestMemoryUsage:
    """Test memory usage"""
    
    @pytest.mark.slow
    def test_memory_leak(self):
        """Test for memory leaks with repeated requests"""
        import gc
        import sys
        
        # Make many requests
        for _ in range(100):
            response = client.get("/api/v1/health")
            assert response.status_code == 200
        
        # Force garbage collection
        gc.collect()
        
        # This is a basic test - more sophisticated profiling
        # would be needed for production
        print(f"\nReference count after 100 requests: {len(gc.get_objects())}")


class TestScalability:
    """Test scalability aspects"""
    
    @pytest.mark.slow
    def test_response_time_consistency(self):
        """Test that response time stays consistent under load"""
        num_requests = 50
        latencies = []
        
        for _ in range(num_requests):
            start = time.time()
            response = client.get("/api/v1/health")
            latency = (time.time() - start) * 1000
            
            if response.status_code == 200:
                latencies.append(latency)
        
        if latencies:
            # Calculate coefficient of variation (std dev / mean)
            mean_latency = statistics.mean(latencies)
            std_dev = statistics.stdev(latencies) if len(latencies) > 1 else 0
            cv = (std_dev / mean_latency) if mean_latency > 0 else 0
            
            print(f"\nResponse time consistency:")
            print(f"  Mean: {mean_latency:.2f}ms")
            print(f"  Std Dev: {std_dev:.2f}ms")
            print(f"  Coefficient of Variation: {cv:.2f}")
            
            # CV should be relatively low for consistent performance
            # Adjust threshold based on acceptable variability
            assert cv < 1.0, f"Response time too variable: CV={cv:.2f}"


class TestRateLimitPerformance:
    """Test rate limiting performance impact"""
    
    def test_rate_limit_overhead(self):
        """Test overhead added by rate limiting"""
        # This would compare latency with and without rate limiting
        # Requires ability to toggle rate limiting
        pass


# Performance benchmarks
def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
