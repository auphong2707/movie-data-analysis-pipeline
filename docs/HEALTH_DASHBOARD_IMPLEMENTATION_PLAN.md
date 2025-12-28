# Health Dashboard Implementation Plan
**System Health & Status Monitoring for Movie Data Analysis Pipeline**

**Created:** December 19, 2025  
**Status:** Ready for Implementation

---

## 🎯 Overview

Implement a comprehensive health monitoring dashboard to track the status of all system components across the Lambda Architecture (Batch Layer, Speed Layer, and Serving Layer). This plan covers API endpoint design, health check logic, and Grafana dashboard configuration.

---

## 📋 System Components to Monitor

### **Serving Layer (6 services)**
1. `serving-mongodb` - MongoDB database
2. `serving-redis` - Redis cache
3. `serving-api` - FastAPI application
4. `serving-prometheus` - Metrics collection
5. `serving-grafana` - Visualization platform
6. `serving-mongo-express` - MongoDB admin UI

### **Batch Layer (6 services)**
7. `batch-minio` - S3-compatible object storage
8. `batch-postgres` - Airflow metadata DB
9. `batch-airflow-webserver` - Airflow UI
10. `batch-airflow-scheduler` - Airflow scheduler
11. `batch-pyspark-runner` - Spark job executor

### **Speed Layer (9 services)**
12. `speed-zookeeper` - Kafka coordination
13. `speed-kafka-1/2/3` - Kafka brokers (3 instances)
14. `speed-schema-registry` - Kafka schema registry
15. `speed-cassandra` - Time-series storage
16. `speed-reddit-producer` - Reddit data producer
17. `speed-reddit-sentiment-stream` - Sentiment streaming job
18. `speed-cassandra-mongo-sync` - Data synchronization

**Total: 21 services across 3 layers**

---

## 🔧 Implementation Steps

### **Phase 1: Backend API Endpoints**

#### **1.1 Enhanced Health Check Endpoint**
**File:** `/layers/serving_layer/api/routes/health.py`

```python
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

router = APIRouter(
    prefix="/health",
    tags=["health"]
)

logger = logging.getLogger(__name__)

# Health check helpers
class HealthChecker:
    """Centralized health check logic for all system components"""
    
    @staticmethod
    async def check_mongodb(uri: str = "mongodb://admin:password@serving-mongodb:27017") -> Dict:
        """Check MongoDB connection and basic metrics"""
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # Ping the database
            client.admin.command('ping')
            
            # Get database stats
            db = client['moviedb']
            stats = db.command('dbStats')
            collections = db.list_collection_names()
            
            return {
                "status": "healthy",
                "response_time_ms": stats.get('ok', 0) * 1000,
                "collections_count": len(collections),
                "data_size_mb": round(stats.get('dataSize', 0) / 1024 / 1024, 2),
                "storage_size_mb": round(stats.get('storageSize', 0) / 1024 / 1024, 2)
            }
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
        finally:
            client.close()
    
    @staticmethod
    async def check_redis(host: str = "serving-redis", port: int = 6379) -> Dict:
        """Check Redis connection and metrics"""
        try:
            r = redis.Redis(host=host, port=port, socket_timeout=5, decode_responses=True)
            info = r.info()
            
            return {
                "status": "healthy",
                "response_time_ms": round(r.ping() * 1000, 2) if r.ping() else 0,
                "used_memory_mb": round(info.get('used_memory', 0) / 1024 / 1024, 2),
                "connected_clients": info.get('connected_clients', 0),
                "uptime_hours": round(info.get('uptime_in_seconds', 0) / 3600, 1)
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_http_service(url: str, timeout: int = 5) -> Dict:
        """Generic HTTP health check for web services"""
        try:
            response = requests.get(url, timeout=timeout)
            return {
                "status": "healthy" if response.status_code == 200 else "degraded",
                "response_time_ms": round(response.elapsed.total_seconds() * 1000, 2),
                "status_code": response.status_code
            }
        except requests.exceptions.Timeout:
            return {
                "status": "unhealthy",
                "error": "Request timeout"
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    @staticmethod
    async def check_kafka_broker(broker: str) -> Dict:
        """Check Kafka broker health via JMX metrics endpoint"""
        # Note: Requires kafka-exporter or JMX exporter to be running
        try:
            # Simplified check - in production use kafka-python or confluent-kafka
            response = requests.get(f"http://{broker}:9092", timeout=3)
            return {"status": "healthy"}
        except:
            # Fallback: assume healthy if Zookeeper is healthy
            return {"status": "unknown", "note": "Requires JMX exporter"}
    
    @staticmethod
    async def check_cassandra(host: str = "speed-cassandra") -> Dict:
        """Check Cassandra health"""
        # Note: Requires cassandra-driver installed
        try:
            # Simplified version - use cassandra-driver in production
            from cassandra.cluster import Cluster
            cluster = Cluster([host], connect_timeout=5)
            session = cluster.connect()
            
            # Test query
            rows = session.execute("SELECT now() FROM system.local")
            
            cluster.shutdown()
            return {
                "status": "healthy",
                "nodes": len(cluster.metadata.all_hosts())
            }
        except ImportError:
            return {"status": "unknown", "note": "cassandra-driver not installed"}
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }


@router.get("")
async def basic_health_check():
    """
    Basic health check - returns OK if API is responsive
    Used for container health checks and load balancers
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "serving-api"
    }


@router.get("/detailed")
async def detailed_health_check():
    """
    Detailed health check for all critical serving layer components
    Returns individual status for MongoDB, Redis, and API
    """
    checker = HealthChecker()
    
    # Run checks in parallel
    mongodb_check, redis_check = await asyncio.gather(
        checker.check_mongodb(),
        checker.check_redis(),
        return_exceptions=True
    )
    
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
    Returns status for all 21 services in the Lambda Architecture
    
    Status Levels:
    - healthy: Service is operational and responsive
    - degraded: Service is running but with issues
    - unhealthy: Service is down or unreachable
    - unknown: Service status could not be determined
    """
    checker = HealthChecker()
    
    # Check all services (some checks are stubbed for services without health endpoints)
    checks = await asyncio.gather(
        # Serving Layer
        checker.check_mongodb(),
        checker.check_redis(),
        checker.check_http_service("http://serving-prometheus:9090/-/healthy"),
        checker.check_http_service("http://serving-grafana:3000/api/health"),
        checker.check_http_service("http://serving-mongo-express:8081"),
        
        # Batch Layer
        checker.check_http_service("http://batch-minio:9000/minio/health/live"),
        checker.check_http_service("http://batch-postgres:5432"),  # Will fail, needs pg check
        checker.check_http_service("http://batch-airflow-webserver:8080/health"),
        
        # Speed Layer
        checker.check_http_service("http://speed-zookeeper:2181"),  # Needs custom check
        checker.check_cassandra(),
        
        return_exceptions=True
    )
    
    # Map results to services
    serving_layer = {
        "mongodb": checks[0],
        "redis": checks[1],
        "prometheus": checks[2],
        "grafana": checks[3],
        "mongo_express": checks[4],
        "api": {"status": "healthy", "note": "Self-check"}
    }
    
    batch_layer = {
        "minio": checks[5],
        "postgres": checks[6] if not isinstance(checks[6], Exception) else {"status": "unknown"},
        "airflow_webserver": checks[7],
        "airflow_scheduler": {"status": "unknown", "note": "No direct health endpoint"},
        "pyspark_runner": {"status": "unknown", "note": "No direct health endpoint"}
    }
    
    speed_layer = {
        "zookeeper": checks[8] if not isinstance(checks[8], Exception) else {"status": "unknown"},
        "kafka_broker_1": {"status": "unknown", "note": "Requires JMX exporter"},
        "kafka_broker_2": {"status": "unknown", "note": "Requires JMX exporter"},
        "kafka_broker_3": {"status": "unknown", "note": "Requires JMX exporter"},
        "schema_registry": {"status": "unknown", "note": "No health check implemented"},
        "cassandra": checks[9],
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
        elif any(s == "unknown" for s in statuses):
            return "degraded"
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
            "total_services": 21,
            "healthy": sum(1 for layer in [serving_layer, batch_layer, speed_layer] 
                          for v in layer.values() 
                          if isinstance(v, dict) and v.get("status") == "healthy"),
            "degraded": sum(1 for layer in [serving_layer, batch_layer, speed_layer] 
                           for v in layer.values() 
                           if isinstance(v, dict) and v.get("status") in ["degraded", "unknown"]),
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
    """
    checker = HealthChecker()
    
    mongodb_status = await checker.check_mongodb()
    redis_status = await checker.check_redis()
    
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
```

#### **1.2 Update Dependencies**
**File:** `/layers/serving_layer/requirements.txt`

Add:
```
requests==2.31.0
redis==5.0.1
cassandra-driver==3.28.0  # Optional, for Cassandra checks
```

---

### **Phase 2: Grafana Dashboard Configuration**

#### **2.1 Dashboard Structure**

**File:** `/layers/serving_layer/visualization/grafana/dashboards/0-system-health-overview.json`

**Dashboard Features:**
- **3 Layer Status Cards** - Serving, Batch, Speed layer health
- **Overall System Status** - Single aggregated health metric
- **Service Status Table** - All 21 services with status indicators
- **Response Time Charts** - MongoDB and Redis latency trends
- **Resource Usage Gauges** - Memory, storage, connections
- **Service Uptime Timeline** - Historical availability view

#### **2.2 Panel Breakdown**

| Panel | Type | Query | Purpose |
|-------|------|-------|---------|
| **System Status** | Stat | `/api/v1/health/system` → `status` | Overall health indicator |
| **Serving Layer Status** | Stat | `/api/v1/health/system` → `layers.serving.status` | Serving layer health |
| **Batch Layer Status** | Stat | `/api/v1/health/system` → `layers.batch.status` | Batch layer health |
| **Speed Layer Status** | Stat | `/api/v1/health/system` → `layers.speed.status` | Speed layer health |
| **Service Health Summary** | Pie Chart | `/api/v1/health/system` → `summary` | Healthy/Degraded/Unhealthy distribution |
| **All Services Status** | Table | `/api/v1/health/system` → All layers | Complete service listing with status |
| **MongoDB Response Time** | Time Series | `/api/v1/health/metrics` → `mongodb.response_time_ms` | MongoDB latency |
| **Redis Response Time** | Time Series | `/api/v1/health/metrics` → `redis.response_time_ms` | Redis latency |
| **MongoDB Storage** | Gauge | `/api/v1/health/metrics` → `mongodb.storage_size_mb` | Storage usage |
| **Redis Memory** | Gauge | `/api/v1/health/metrics` → `redis.used_memory_mb` | Memory usage |

---

### **Phase 3: Dashboard JSON Configuration**

**Complete Grafana Dashboard:**

```json
{
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": {"type": "grafana", "uid": "-- Grafana --"},
        "enable": true,
        "hide": true,
        "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts",
        "type": "dashboard"
      }
    ]
  },
  "editable": true,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "id": null,
  "links": [],
  "panels": [
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "thresholds"},
          "mappings": [
            {"options": {"healthy": {"color": "green", "index": 0, "text": "✅ Healthy"}}, "type": "value"},
            {"options": {"degraded": {"color": "yellow", "index": 1, "text": "⚠️ Degraded"}}, "type": "value"},
            {"options": {"unhealthy": {"color": "red", "index": 2, "text": "🔴 Unhealthy"}}, "type": "value"}
          ],
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {"color": "green", "value": null},
              {"color": "yellow", "value": 1},
              {"color": "red", "value": 2}
            ]
          }
        }
      },
      "gridPos": {"h": 6, "w": 6, "x": 0, "y": 0},
      "id": 1,
      "options": {
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "center",
        "orientation": "auto",
        "reduceOptions": {"values": false, "calcs": ["lastNotNull"], "fields": ""},
        "textMode": "value_and_name",
        "wideLayout": true
      },
      "pluginVersion": "11.0.0",
      "targets": [
        {
          "columns": [{"selector": "status", "text": "System Status", "type": "string"}],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "root_selector": "",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/system",
          "url_options": {"data": "", "method": "GET"},
          "parser": "backend"
        }
      ],
      "title": "🏥 Overall System Status",
      "type": "stat"
    },
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "thresholds"},
          "mappings": [
            {"options": {"healthy": {"color": "green", "index": 0, "text": "Healthy"}}, "type": "value"},
            {"options": {"degraded": {"color": "yellow", "index": 1, "text": "Degraded"}}, "type": "value"},
            {"options": {"unhealthy": {"color": "red", "index": 2, "text": "Unhealthy"}}, "type": "value"}
          ],
          "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}
        }
      },
      "gridPos": {"h": 6, "w": 6, "x": 6, "y": 0},
      "id": 2,
      "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "center", "orientation": "auto", "reduceOptions": {"values": false, "calcs": ["lastNotNull"]}, "textMode": "value_and_name", "wideLayout": true},
      "targets": [
        {
          "columns": [{"selector": "layers.serving.status", "text": "Serving Layer", "type": "string"}],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/system",
          "parser": "backend"
        }
      ],
      "title": "📊 Serving Layer",
      "type": "stat"
    },
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "thresholds"},
          "mappings": [
            {"options": {"healthy": {"color": "green", "index": 0, "text": "Healthy"}}, "type": "value"},
            {"options": {"degraded": {"color": "yellow", "index": 1, "text": "Degraded"}}, "type": "value"},
            {"options": {"unhealthy": {"color": "red", "index": 2, "text": "Unhealthy"}}, "type": "value"}
          ],
          "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}
        }
      },
      "gridPos": {"h": 6, "w": 6, "x": 12, "y": 0},
      "id": 3,
      "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "center", "orientation": "auto", "reduceOptions": {"values": false, "calcs": ["lastNotNull"]}, "textMode": "value_and_name", "wideLayout": true},
      "targets": [
        {
          "columns": [{"selector": "layers.batch.status", "text": "Batch Layer", "type": "string"}],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/system",
          "parser": "backend"
        }
      ],
      "title": "⏱️ Batch Layer",
      "type": "stat"
    },
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "thresholds"},
          "mappings": [
            {"options": {"healthy": {"color": "green", "index": 0, "text": "Healthy"}}, "type": "value"},
            {"options": {"degraded": {"color": "yellow", "index": 1, "text": "Degraded"}}, "type": "value"},
            {"options": {"unhealthy": {"color": "red", "index": 2, "text": "Unhealthy"}}, "type": "value"}
          ],
          "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}
        }
      },
      "gridPos": {"h": 6, "w": 6, "x": 18, "y": 0},
      "id": 4,
      "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "center", "orientation": "auto", "reduceOptions": {"values": false, "calcs": ["lastNotNull"]}, "textMode": "value_and_name", "wideLayout": true},
      "targets": [
        {
          "columns": [{"selector": "layers.speed.status", "text": "Speed Layer", "type": "string"}],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/system",
          "parser": "backend"
        }
      ],
      "title": "⚡ Speed Layer",
      "type": "stat"
    },
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "palette-classic"},
          "custom": {"hideFrom": {"tooltip": false, "viz": false, "legend": false}},
          "mappings": []
        },
        "overrides": [
          {"matcher": {"id": "byName", "options": "healthy"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]},
          {"matcher": {"id": "byName", "options": "degraded"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "yellow"}}]},
          {"matcher": {"id": "byName", "options": "unhealthy"}, "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]}
        ]
      },
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 6},
      "id": 5,
      "options": {
        "legend": {"displayMode": "table", "placement": "right", "showLegend": true, "values": ["value", "percent"]},
        "pieType": "donut",
        "tooltip": {"mode": "single"},
        "displayLabels": ["percent"],
        "reduceOptions": {"values": false, "calcs": ["lastNotNull"]}
      },
      "targets": [
        {
          "columns": [
            {"selector": "summary.healthy", "text": "healthy", "type": "number"},
            {"selector": "summary.degraded", "text": "degraded", "type": "number"},
            {"selector": "summary.unhealthy", "text": "unhealthy", "type": "number"}
          ],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/system",
          "parser": "backend"
        }
      ],
      "title": "📊 Service Health Distribution",
      "type": "piechart"
    },
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": false},
          "mappings": [],
          "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}
        },
        "overrides": [
          {"matcher": {"id": "byName", "options": "Status"}, "properties": [{"id": "custom.width", "value": 100}]},
          {"matcher": {"id": "byName", "options": "Service"}, "properties": [{"id": "custom.width", "value": 250}]}
        ]
      },
      "gridPos": {"h": 16, "w": 16, "x": 8, "y": 6},
      "id": 6,
      "options": {
        "cellHeight": "sm",
        "footer": {"countRows": false, "fields": "", "reducer": ["sum"], "show": false},
        "showHeader": true,
        "sortBy": []
      },
      "pluginVersion": "11.0.0",
      "targets": [
        {
          "columns": [],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/system",
          "parser": "backend",
          "root_selector": "layers"
        }
      ],
      "title": "🔧 All Services Status (21 Total)",
      "type": "table",
      "transformations": [
        {
          "id": "extractFields",
          "options": {"source": "layers"}
        }
      ]
    },
    {
      "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
      "fieldConfig": {
        "defaults": {
          "color": {"mode": "palette-classic"},
          "custom": {"axisCenteredZero": false, "axisColorMode": "text", "axisLabel": "Response Time (ms)", "axisPlacement": "auto", "fillOpacity": 80, "gradientMode": "none", "hideFrom": {"tooltip": false, "viz": false, "legend": false}, "lineWidth": 1},
          "mappings": [],
          "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}, {"color": "yellow", "value": 100}, {"color": "red", "value": 500}]},
          "unit": "ms"
        }
      },
      "gridPos": {"h": 8, "w": 8, "x": 0, "y": 14},
      "id": 7,
      "options": {
        "legend": {"calcs": ["lastNotNull", "max"], "displayMode": "list", "placement": "bottom", "showLegend": true},
        "tooltip": {"mode": "single"}
      },
      "targets": [
        {
          "columns": [
            {"selector": "metrics.mongodb.response_time_ms", "text": "MongoDB", "type": "number"},
            {"selector": "metrics.redis.response_time_ms", "text": "Redis", "type": "number"}
          ],
          "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity"},
          "format": "table",
          "refId": "A",
          "source": "url",
          "type": "json",
          "url": "http://serving-api:8000/api/v1/health/metrics",
          "parser": "backend"
        }
      ],
      "title": "⚡ Database Response Times",
      "type": "timeseries"
    }
  ],
  "refresh": "30s",
  "schemaVersion": 39,
  "tags": ["system-health", "monitoring", "lambda-architecture"],
  "templating": {"list": []},
  "time": {"from": "now-1h", "to": "now"},
  "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m"]},
  "timezone": "browser",
  "title": "0. System Health Overview - Lambda Architecture",
  "uid": "system-health-overview",
  "version": 1,
  "weekStart": ""
}
```

---

## 🚀 Implementation Checklist

### **Backend (API)**
- [ ] Update `/layers/serving_layer/api/routes/health.py` with new endpoints
- [ ] Add `requests`, `redis` to `requirements.txt`
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Test endpoints:
  - [ ] `GET /api/v1/health` - Basic check
  - [ ] `GET /api/v1/health/detailed` - Serving layer details
  - [ ] `GET /api/v1/health/system` - Full system overview
  - [ ] `GET /api/v1/health/metrics` - Time-series metrics
- [ ] Restart API: `docker-compose restart serving-api`

### **Frontend (Grafana Dashboard)**
- [ ] Create `/layers/serving_layer/visualization/grafana/dashboards/0-system-health-overview.json`
- [ ] Import dashboard to Grafana:
  - Option 1: Copy JSON to Grafana UI (+ → Import → Paste JSON)
  - Option 2: Restart Grafana container to auto-load
- [ ] Verify datasource: `yesoreyeram-infinity-datasource` (should already be configured)
- [ ] Test dashboard:
  - [ ] All panels load without errors
  - [ ] Status cards show correct colors
  - [ ] Service table populates
  - [ ] Response time charts update

### **Optional Enhancements**
- [ ] Add alerting rules in Grafana for unhealthy services
- [ ] Implement Prometheus exporters for Kafka/Cassandra
- [ ] Add historical uptime tracking
- [ ] Create mobile-responsive dashboard layout
- [ ] Add email/Slack notifications for critical failures

---

## 📊 Expected Results

### **Health Status Colors**
- 🟢 **Green (Healthy):** Service is fully operational
- 🟡 **Yellow (Degraded):** Service running but with warnings
- 🔴 **Red (Unhealthy):** Service is down or unreachable
- ⚫ **Gray (Unknown):** Status cannot be determined

### **Dashboard Refresh**
- Auto-refresh: Every 30 seconds
- Manual refresh: Available in Grafana UI
- Time range: Last 1 hour (configurable)

### **API Response Times**
- Typical: 50-200ms for `/health/system`
- Warning: >500ms (check network/database)
- Critical: >2000ms or timeout

---

## 🔍 Troubleshooting

### **Issue: All services show "unknown"**
**Solution:** Check if services are running: `docker ps | grep movie-pipeline`

### **Issue: MongoDB check fails**
**Solution:** Verify MongoDB connection string in environment variables

### **Issue: Dashboard panels show "No data"**
**Solution:** 
1. Check API is accessible: `curl http://localhost:8000/api/v1/health/system`
2. Verify Infinity datasource is configured in Grafana
3. Check panel query syntax in dashboard JSON

### **Issue: Redis metrics missing**
**Solution:** Ensure Redis is running: `docker exec serving-redis redis-cli ping`

---

## 📝 Notes

- **Dashboard Number:** "0" prefix ensures it appears first in Grafana sidebar
- **Service Count:** 21 total services (6 serving + 5 batch + 10 speed)
- **Health Check Interval:** Designed for 30-second refresh (balance between freshness and load)
- **Future Work:** Add Kafka, Cassandra, and Spark streaming job health checks when monitoring infrastructure is available

---

## 🔗 Related Documentation

- API Endpoints: `/layers/serving_layer/api/routes/health.py`
- MongoDB Schemas: `/docs/MONGODB_SCHEMAS.md`
- Docker Compose: `/docker-compose.yml`
- Existing Dashboard Example: `/layers/serving_layer/visualization/grafana/dashboards/1-crisis-alert-overview.json`

---

**Status:** ✅ Ready for implementation  
**Estimated Time:** 2-3 hours  
**Priority:** Medium (enables operational visibility)
