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
import redis.asyncio as aioredis
import requests
import os
import socket

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
                "status_code": 200,
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
    async def check_postgres(host: str = None, port: int = 5432, 
                            user: str = None, password: str = None, 
                            database: str = None) -> Dict:
        """Check PostgreSQL connection"""
        if host is None:
            host = os.getenv('POSTGRES_HOST', 'batch-postgres')
        if user is None:
            user = os.getenv('POSTGRES_USER', 'airflow')
        if password is None:
            password = os.getenv('POSTGRES_PASSWORD', 'airflow')
        if database is None:
            database = os.getenv('POSTGRES_DB', 'airflow')
        
        try:
            import psycopg2
            
            start_time = datetime.now()
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                connect_timeout=5
            )
            cur = conn.cursor()
            cur.execute('SELECT version();')
            version = cur.fetchone()[0]
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get database stats
            cur.execute('SELECT pg_database_size(current_database());')
            db_size = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            return {
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "status_code": 200,
                "version": version.split(',')[0].replace('PostgreSQL ', ''),
                "database_size_mb": round(db_size / 1024 / 1024, 2)
            }
        except ImportError:
            logger.warning("psycopg2 not installed, using TCP check for PostgreSQL")
            return await HealthChecker.check_tcp_port(host, port, "PostgreSQL")
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_cassandra(hosts: list = None, port: int = 9042, keyspace: str = None) -> Dict:
        """Check Cassandra connection"""
        if hosts is None:
            hosts = [os.getenv('CASSANDRA_HOST', 'speed-cassandra')]
        if keyspace is None:
            keyspace = os.getenv('CASSANDRA_KEYSPACE', 'speed_layer')
        
        try:
            from cassandra.cluster import Cluster
            from cassandra.policies import RoundRobinPolicy
            
            start_time = datetime.now()
            cluster = Cluster(
                hosts,
                port=port,
                load_balancing_policy=RoundRobinPolicy(),
                connect_timeout=5
            )
            session = cluster.connect()
            
            # Query cluster metadata
            result = session.execute("SELECT release_version FROM system.local")
            version = result.one()[0]
            
            # Check if keyspace exists
            keyspaces = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
            keyspace_names = [row.keyspace_name for row in keyspaces]
            keyspace_exists = keyspace in keyspace_names
            
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            cluster.shutdown()
            
            return {
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "status_code": 200,
                "version": version,
                "keyspace": keyspace,
                "keyspace_exists": keyspace_exists
            }
        except ImportError:
            logger.warning("cassandra-driver not installed, using TCP check for Cassandra")
            return await HealthChecker.check_tcp_port(hosts[0] if hosts else 'speed-cassandra', port, "Cassandra")
        except Exception as e:
            logger.error(f"Cassandra health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_tcp_port(host: str, port: int, service_name: str = None, timeout: int = 5) -> Dict:
        """Generic TCP port connectivity check"""
        try:
            start_time = datetime.now()
            
            # Use asyncio for non-blocking socket connection
            loop = asyncio.get_event_loop()
            future = loop.run_in_executor(None, socket.create_connection, (host, port), timeout)
            sock = await asyncio.wait_for(future, timeout=timeout)
            
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            sock.close()
            
            return {
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "status_code": 200,
                "note": f"TCP port {port} is open"
            }
        except asyncio.TimeoutError:
            return {
                "status": "unhealthy",
                "error": f"Connection timeout to {host}:{port}"
            }
        except Exception as e:
            logger.error(f"TCP health check failed for {service_name or host}:{port} - {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_redis(host: str = None, port: int = 6379) -> Dict:
        """Check Redis connection and metrics using async client"""
        if host is None:
            host = os.getenv('REDIS_HOST', 'serving-redis')
        
        try:
            # Create async Redis client
            redis_url = f"redis://{host}:{port}/0"
            client = aioredis.from_url(
                redis_url,
                socket_timeout=5,
                socket_connect_timeout=5,
                decode_responses=True
            )
            
            # Measure ping time
            start_time = datetime.now()
            await client.ping()
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Get server info
            info = await client.info()
            stats = await client.info('stats')
            
            # Calculate hit rate
            hits = stats.get('keyspace_hits', 0)
            misses = stats.get('keyspace_misses', 0)
            total_requests = hits + misses
            hit_rate = round((hits / total_requests * 100) if total_requests > 0 else 0, 2)
            
            await client.close()
            
            return {
                "status": "healthy",
                "response_time_ms": round(response_time, 2),
                "status_code": 200,
                "used_memory_mb": round(info.get('used_memory', 0) / 1024 / 1024, 2),
                "connected_clients": info.get('connected_clients', 0),
                "uptime_hours": round(info.get('uptime_in_seconds', 0) / 3600, 1),
                "hit_rate_percent": hit_rate,
                "version": info.get('redis_version', 'unknown')
            }
        except aioredis.ConnectionError as e:
            logger.error(f"Redis health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": "Connection failed",
                "message": str(e)
            }
        except Exception as e:
            logger.error(f"Redis health check error: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_http_service(url: str, timeout: int = 5, auth: tuple = None) -> Dict:
        """Generic HTTP health check for web services"""
        try:
            start_time = datetime.now()
            response = requests.get(url, timeout=timeout, auth=auth)
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
        checker.check_http_service("http://serving-mongo-express:8081", auth=("admin", "admin")),
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
    minio_check, postgres_check, airflow_check, airflow_scheduler_check = await asyncio.gather(
        checker.check_http_service("http://batch-minio:9000/minio/health/live"),
        checker.check_postgres(),
        checker.check_http_service("http://batch-airflow-webserver:8080/health"),
        checker.check_tcp_port("batch-airflow-scheduler", 8793, "Airflow Scheduler"),
        return_exceptions=True
    )
    
    minio_check = safe_check(minio_check)
    postgres_check = safe_check(postgres_check)
    airflow_check = safe_check(airflow_check)
    airflow_scheduler_check = safe_check(airflow_scheduler_check)
    
    # Check speed layer infrastructure
    zookeeper_check, schema_registry_check, cassandra_check = await asyncio.gather(
        checker.check_tcp_port("speed-zookeeper", 2181, "Zookeeper"),
        checker.check_http_service("http://speed-schema-registry:8081/"),
        checker.check_cassandra(),
        return_exceptions=True
    )
    
    zookeeper_check = safe_check(zookeeper_check)
    schema_registry_check = safe_check(schema_registry_check)
    cassandra_check = safe_check(cassandra_check)
    
    # Check Kafka brokers via TCP
    kafka1_check, kafka2_check, kafka3_check = await asyncio.gather(
        checker.check_tcp_port("speed-kafka-1", 29092, "Kafka Broker 1"),
        checker.check_tcp_port("speed-kafka-2", 29092, "Kafka Broker 2"),
        checker.check_tcp_port("speed-kafka-3", 29092, "Kafka Broker 3"),
        return_exceptions=True
    )
    
    kafka1_check = safe_check(kafka1_check)
    kafka2_check = safe_check(kafka2_check)
    kafka3_check = safe_check(kafka3_check)
    
    # Check speed layer processing services
    # Note: Some services are pure Python scripts without HTTP endpoints
    # Only Spark-based jobs expose port 4040
    sentiment_stream_check, = await asyncio.gather(
        # Sentiment stream has Spark UI on port 4040
        checker.check_tcp_port("speed-reddit-sentiment-stream", 4040, "Sentiment Stream Spark UI"),
        return_exceptions=True
    )
    
    # For services without HTTP endpoints, we'll check their dependencies as a proxy
    # Reddit producer: Plain Python script (no Spark, no HTTP) - check Kafka connectivity as proxy
    # Cassandra sync: Plain Python script - check Cassandra connectivity (already done)
    # PySpark runner: Job-based service - may not always be running, mark as degraded
    
    # PySpark runner is a job executor that may not be actively running
    pyspark_runner_check = {
        "status": "degraded" if postgres_check.get("status") == "healthy" else "unknown",
        "note": "⚠️ Job-based service - runs on demand. Cannot directly monitor without health endpoint. PostgreSQL is " + postgres_check.get("status", "unknown") + ". Recommend: Add HTTP health endpoint or check job execution logs."
    }
    
    # For reddit producer and cassandra sync: mark as degraded if Kafka/Cassandra are healthy
    # This is an indirect health check - better than hardcoded "healthy"
    reddit_producer_check = {
        "status": "degraded" if kafka1_check.get("status") == "healthy" else "unknown",
        "note": "⚠️ Cannot directly monitor - no health endpoint. Kafka is " + kafka1_check.get("status", "unknown") + ". Recommend: Add HTTP health endpoint or mount Docker socket for container health checks."
    }
    
    cassandra_sync_check = {
        "status": "degraded" if cassandra_check.get("status") == "healthy" else "unknown",
        "note": "⚠️ Cannot directly monitor - no health endpoint. Cassandra is " + cassandra_check.get("status", "unknown") + ". Recommend: Add HTTP health endpoint or mount Docker socket for container health checks."
    }
    
    reddit_producer_check = safe_check(reddit_producer_check)
    sentiment_stream_check = safe_check(sentiment_stream_check)
    cassandra_sync_check = safe_check(cassandra_sync_check)
    pyspark_runner_check = safe_check(pyspark_runner_check)
    
    # Build layer status
    serving_layer = {
        "mongodb": mongodb_check,
        "redis": redis_check,
        "prometheus": prometheus_check,
        "grafana": grafana_check,
        "mongo_express": mongo_express_check,
        "api": {"status": "healthy", "status_code": 200, "note": "Self-check"}
    }
    
    batch_layer = {
        "minio": minio_check,
        "postgres": postgres_check,
        "airflow_webserver": airflow_check,
        "airflow_scheduler": airflow_scheduler_check,
        "pyspark_runner": pyspark_runner_check
    }
    
    speed_layer = {
        "zookeeper": zookeeper_check,
        "kafka_broker_1": kafka1_check,
        "kafka_broker_2": kafka2_check,
        "kafka_broker_3": kafka3_check,
        "schema_registry": schema_registry_check,
        "cassandra": cassandra_check,
        "reddit_producer": reddit_producer_check,
        "sentiment_stream": sentiment_stream_check,
        "cassandra_sync": cassandra_sync_check
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


@router.get("/status-simple")
async def simple_status_overview():
    """
    Simplified status endpoint returning numeric status codes
    Optimized for Grafana stat panels with the Infinity datasource
    
    Status codes:
    - 0: healthy
    - 1: degraded
    - 2: unhealthy
    - 3: unknown
    
    Returns:
        dict: Simple status with numeric codes for each layer and overall system
    """
    system_health = await system_health_overview()
    
    # Map status strings to numeric codes
    status_map = {
        "healthy": 0,
        "degraded": 1,
        "unhealthy": 2,
        "unknown": 3
    }
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system_status": status_map.get(system_health["status"], 3),
        "system_status_text": system_health["status"],
        "batch_status": status_map.get(system_health["layers"]["batch"]["status"], 3),
        "batch_status_text": system_health["layers"]["batch"]["status"],
        "speed_status": status_map.get(system_health["layers"]["speed"]["status"], 3),
        "speed_status_text": system_health["layers"]["speed"]["status"],
        "serving_status": status_map.get(system_health["layers"]["serving"]["status"], 3),
        "serving_status_text": system_health["layers"]["serving"]["status"],
        "total_services": system_health["summary"]["total_services"],
        "healthy_count": system_health["summary"]["healthy"],
        "degraded_count": system_health["summary"]["degraded"],
        "unhealthy_count": system_health["summary"]["unhealthy"],
        "unknown_count": system_health["summary"]["unknown"]
    }


@router.get("/layer-status")
async def layer_status_array():
    """
    Layer status as an array for easy consumption by Grafana
    Returns each layer as an array element with status information
    
    Returns:
        dict: Array of layer statuses
    """
    system_health = await system_health_overview()
    
    # Map status strings to numeric codes
    status_map = {
        "healthy": 0,
        "degraded": 1,
        "unhealthy": 2,
        "unknown": 3
    }
    
    layers_array = []
    
    for layer_name, layer_data in system_health["layers"].items():
        layer_display = {
            "serving": "Serving",
            "batch": "Batch", 
            "speed": "Speed"
        }.get(layer_name, layer_name.title())
        
        layers_array.append({
            "layer": layer_display,
            "status": layer_data["status"],
            "status_code": status_map.get(layer_data["status"], 3),
            "total_services": layer_data["total_services"],
            "healthy_count": layer_data["healthy_count"]
        })
    
    # Add overall system as a layer
    layers_array.insert(0, {
        "layer": "Overall System",
        "status": system_health["status"],
        "status_code": status_map.get(system_health["status"], 3),
        "total_services": system_health["summary"]["total_services"],
        "healthy_count": system_health["summary"]["healthy"]
    })
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "layers": layers_array
    }


@router.get("/collection-counts")
async def collection_document_counts():
    """
    Get document counts for all MongoDB collections
    Returns the number of data points in each collection
    
    Returns:
        dict: Document counts for each collection
    """
    uri = os.getenv('MONGODB_URI', 'mongodb://admin:password@serving-mongodb:27017')
    db_name = os.getenv('MONGODB_DATABASE', 'moviedb')
    
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        db = client[db_name]
        
        # Define the collections to count
        collections = [
            "movie_intelligence",
            "speed_views", 
            "viral_thresholds",
            "sentiment_baselines"
        ]
        
        counts = {}
        for collection_name in collections:
            try:
                count = db[collection_name].count_documents({})
                counts[collection_name] = count
            except Exception as e:
                logger.error(f"Error counting documents in {collection_name}: {e}")
                counts[collection_name] = 0
        
        client.close()
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": db_name,
            "collections": counts,
            "total_documents": sum(counts.values())
        }
    except Exception as e:
        logger.error(f"Error getting collection counts: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Failed to retrieve collection counts: {str(e)}"
        )
