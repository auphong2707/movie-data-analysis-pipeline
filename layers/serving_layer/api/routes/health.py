"""
Health Check Routes - System Status Monitoring
Comprehensive health checks for Lambda Architecture components
"""
from fastapi import APIRouter, HTTPException
from datetime import datetime, timezone
from typing import Dict, List, Optional
import logging
import asyncio
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import redis
import requests
import os

router = APIRouter(
    prefix="/health",
    tags=["health"]
)

logger = logging.getLogger(__name__)


class HealthChecker:
    """Centralized health check logic for all system components"""
    
    @staticmethod
    async def check_mongodb(uri: str = None) -> Dict:
        """Check MongoDB connection and basic metrics"""
        if uri is None:
            uri = os.getenv('MONGODB_URI', 'mongodb://admin:password@serving-mongodb:27017')
        
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # Ping the database
            start_time = datetime.now()
            client.admin.command('ping')
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get database stats
            db_name = os.getenv('MONGODB_DATABASE', 'moviedb')
            db = client[db_name]
            stats = db.command('dbStats')
            collections = db.list_collection_names()
            
            client.close()
            
            return {
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "collections_count": len(collections),
                "data_size_mb": round(stats.get('dataSize', 0) / 1024 / 1024, 2),
                "storage_size_mb": round(stats.get('storageSize', 0) / 1024 / 1024, 2)
            }
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.error(f"MongoDB health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
        except Exception as e:
            logger.error(f"MongoDB health check error: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_redis(host: str = None, port: int = 6379) -> Dict:
        """Check Redis connection and metrics"""
        if host is None:
            host = os.getenv('REDIS_HOST', 'serving-redis')
        
        try:
            r = redis.Redis(host=host, port=port, socket_timeout=5, decode_responses=True)
            
            # Measure ping time
            start_time = datetime.now()
            r.ping()
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            info = r.info()
            
            r.close()
            
            return {
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "used_memory_mb": round(info.get('used_memory', 0) / 1024 / 1024, 2),
                "connected_clients": info.get('connected_clients', 0),
                "uptime_hours": round(info.get('uptime_in_seconds', 0) / 3600, 1)
            }
        except redis.ConnectionError as e:
            logger.error(f"Redis health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": "Connection failed"
            }
        except Exception as e:
            logger.error(f"Redis health check error: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_http_service(url: str, timeout: int = 5) -> Dict:
        """Generic HTTP health check for web services"""
        try:
            start_time = datetime.now()
            response = requests.get(url, timeout=timeout)
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                "status": "healthy" if response.status_code == 200 else "degraded",
                "response_time_ms": round(response_time, 2),
                "status_code": response.status_code
            }
        except requests.exceptions.Timeout:
            logger.error(f"HTTP health check timeout: {url}")
            return {
                "status": "unhealthy",
                "error": "Request timeout"
            }
        except requests.exceptions.ConnectionError:
            logger.error(f"HTTP health check connection error: {url}")
            return {
                "status": "unhealthy",
                "error": "Connection refused"
            }
        except Exception as e:
            logger.error(f"HTTP health check error for {url}: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }


@router.get("")
async def basic_health_check():
    """
    Basic health check - returns OK if API is responsive
    Used for container health checks and load balancers
    
    Returns:
        dict: Basic health status with timestamp
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "serving-api",
        "version": "2.0.0"
    }


@router.get("/detailed")
async def detailed_health_check():
    """
    Detailed health check for all critical serving layer components
    Returns individual status for MongoDB, Redis, and API
    
    Returns:
        dict: Detailed health status for serving layer components
    """
    checker = HealthChecker()
    
    # Run checks in parallel
    mongodb_check, redis_check = await asyncio.gather(
        checker.check_mongodb(),
        checker.check_redis(),
        return_exceptions=True
    )
    
    # Handle exceptions from gather
    if isinstance(mongodb_check, Exception):
        mongodb_check = {"status": "unhealthy", "error": str(mongodb_check)}
    if isinstance(redis_check, Exception):
        redis_check = {"status": "unhealthy", "error": str(redis_check)}
    
    # Determine overall status
    all_checks = [mongodb_check, redis_check]
    overall_status = "healthy"
    if any(check.get("status") == "unhealthy" for check in all_checks):
        overall_status = "unhealthy"
    elif any(check.get("status") == "degraded" for check in all_checks):
        overall_status = "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "mongodb": mongodb_check,
            "redis": redis_check,
            "api": {
                "status": "healthy",
                "version": "2.0.0"
            }
        }
    }


@router.get("/system")
async def system_health_overview():
    """
    Complete system health overview across all layers
    Returns status for services in the Lambda Architecture
    
    Status Levels:
    - healthy: Service is operational and responsive
    - degraded: Service is running but with issues
    - unhealthy: Service is down or unreachable
    - unknown: Service status could not be determined
    
    Returns:
        dict: Complete system health status across all layers
    """
    checker = HealthChecker()
    
    # Check serving layer services in parallel
    mongodb_check, redis_check, prometheus_check, grafana_check, mongo_express_check = await asyncio.gather(
        checker.check_mongodb(),
        checker.check_redis(),
        checker.check_http_service("http://serving-prometheus:9090/-/healthy"),
        checker.check_http_service("http://serving-grafana:3000/api/health"),
        checker.check_http_service("http://serving-mongo-express:8081"),
        return_exceptions=True
    )
    
    # Handle exceptions
    def safe_check(check):
        return check if not isinstance(check, Exception) else {"status": "unhealthy", "error": str(check)}
    
    mongodb_check = safe_check(mongodb_check)
    redis_check = safe_check(redis_check)
    prometheus_check = safe_check(prometheus_check)
    grafana_check = safe_check(grafana_check)
    mongo_express_check = safe_check(mongo_express_check)
    
    # Check batch layer services
    minio_check, airflow_check = await asyncio.gather(
        checker.check_http_service("http://batch-minio:9000/minio/health/live"),
        checker.check_http_service("http://batch-airflow-webserver:8080/health"),
        return_exceptions=True
    )
    
    minio_check = safe_check(minio_check)
    airflow_check = safe_check(airflow_check)
    
    # Build layer status
    serving_layer = {
        "mongodb": mongodb_check,
        "redis": redis_check,
        "prometheus": prometheus_check,
        "grafana": grafana_check,
        "mongo_express": mongo_express_check,
        "api": {"status": "healthy", "note": "Self-check"}
    }
    
    batch_layer = {
        "minio": minio_check,
        "postgres": {"status": "unknown", "note": "No HTTP health endpoint"},
        "airflow_webserver": airflow_check,
        "airflow_scheduler": {"status": "unknown", "note": "No direct health endpoint"},
        "pyspark_runner": {"status": "unknown", "note": "Job-based service"}
    }
    
    speed_layer = {
        "zookeeper": {"status": "unknown", "note": "No HTTP health endpoint"},
        "kafka_broker_1": {"status": "unknown", "note": "Requires JMX exporter"},
        "kafka_broker_2": {"status": "unknown", "note": "Requires JMX exporter"},
        "kafka_broker_3": {"status": "unknown", "note": "Requires JMX exporter"},
        "schema_registry": {"status": "unknown", "note": "No health check implemented"},
        "cassandra": {"status": "unknown", "note": "Requires cassandra-driver"},
        "reddit_producer": {"status": "unknown", "note": "No health check implemented"},
        "sentiment_stream": {"status": "unknown", "note": "No health check implemented"},
        "cassandra_sync": {"status": "unknown", "note": "No health check implemented"}
    }
    
    # Calculate layer health
    def calculate_layer_health(layer_dict: Dict) -> str:
        statuses = [v.get("status") for v in layer_dict.values() if isinstance(v, dict)]
        if any(s == "unhealthy" for s in statuses):
            return "unhealthy"
        elif any(s == "degraded" for s in statuses):
            return "degraded"
        elif all(s == "unknown" for s in statuses):
            return "unknown"
        return "healthy"
    
    serving_status = calculate_layer_health(serving_layer)
    batch_status = calculate_layer_health(batch_layer)
    speed_status = calculate_layer_health(speed_layer)
    
    # Overall system status
    overall_status = "healthy"
    if any(status == "unhealthy" for status in [serving_status, batch_status, speed_status]):
        overall_status = "unhealthy"
    elif any(status == "degraded" for status in [serving_status, batch_status, speed_status]):
        overall_status = "degraded"
    elif all(status == "unknown" for status in [batch_status, speed_status]):
        overall_status = "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "layers": {
            "serving": {
                "status": serving_status,
                "services": serving_layer,
                "total_services": len(serving_layer),
                "healthy_count": sum(1 for v in serving_layer.values() if isinstance(v, dict) and v.get("status") == "healthy")
            },
            "batch": {
                "status": batch_status,
                "services": batch_layer,
                "total_services": len(batch_layer),
                "healthy_count": sum(1 for v in batch_layer.values() if isinstance(v, dict) and v.get("status") == "healthy")
            },
            "speed": {
                "status": speed_status,
                "services": speed_layer,
                "total_services": len(speed_layer),
                "healthy_count": sum(1 for v in speed_layer.values() if isinstance(v, dict) and v.get("status") == "healthy")
            }
        },
        "summary": {
            "total_services": 20,
            "healthy": sum(1 for layer in [serving_layer, batch_layer, speed_layer] 
                          for v in layer.values() 
                          if isinstance(v, dict) and v.get("status") == "healthy"),
            "degraded": sum(1 for layer in [serving_layer, batch_layer, speed_layer] 
                           for v in layer.values() 
                           if isinstance(v, dict) and v.get("status") in ["degraded"]),
            "unknown": sum(1 for layer in [serving_layer, batch_layer, speed_layer] 
                          for v in layer.values() 
                          if isinstance(v, dict) and v.get("status") == "unknown"),
            "unhealthy": sum(1 for layer in [serving_layer, batch_layer, speed_layer] 
                            for v in layer.values() 
                            if isinstance(v, dict) and v.get("status") == "unhealthy")
        }
    }


@router.get("/metrics")
async def system_metrics():
    """
    Key system metrics for monitoring dashboards
    Returns metrics suitable for time-series visualization
    
    Returns:
        dict: System metrics for MongoDB and Redis
    """
    checker = HealthChecker()
    
    mongodb_status, redis_status = await asyncio.gather(
        checker.check_mongodb(),
        checker.check_redis(),
        return_exceptions=True
    )
    
    # Handle exceptions
    if isinstance(mongodb_status, Exception):
        mongodb_status = {"status": "unhealthy", "error": str(mongodb_status)}
    if isinstance(redis_status, Exception):
        redis_status = {"status": "unhealthy", "error": str(redis_status)}
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "mongodb": {
                "collections_count": mongodb_status.get("collections_count", 0),
                "data_size_mb": mongodb_status.get("data_size_mb", 0),
                "storage_size_mb": mongodb_status.get("storage_size_mb", 0),
                "response_time_ms": mongodb_status.get("response_time_ms", 0)
            },
            "redis": {
                "used_memory_mb": redis_status.get("used_memory_mb", 0),
                "connected_clients": redis_status.get("connected_clients", 0),
                "uptime_hours": redis_status.get("uptime_hours", 0),
                "response_time_ms": redis_status.get("response_time_ms", 0)
            }
        }
    }


@router.get("/services")
async def list_all_services():
    """
    List all services across all layers in a flat array format
    Optimized for Grafana table visualization
    
    Returns:
        dict: Flat list of all services with their status
    """
    # Get the full system health
    system_health = await system_health_overview()
    
    services = []
    
    # Extract services from each layer
    for layer_name, layer_data in system_health["layers"].items():
        layer_display = {
            "serving": "Serving",
            "batch": "Batch", 
            "speed": "Speed"
        }.get(layer_name, layer_name.title())
        
        for service_name, service_data in layer_data["services"].items():
            if isinstance(service_data, dict):
                services.append({
                    "layer": layer_display,
                    "service": service_name.replace("_", " ").title(),
                    "status": service_data.get("status", "unknown"),
                    "response_time_ms": service_data.get("response_time_ms"),
                    "note": service_data.get("note"),
                    "status_code": service_data.get("status_code")
                })
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_services": len(services),
        "services": services
    }
