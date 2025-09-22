# Movie Analytics Pipeline - Kubernetes Deployment

This directory contains Kubernetes manifests for deploying the complete Movie Analytics Pipeline to a Kubernetes cluster.

## 📁 File Overview

| File | Description |
|------|-------------|
| `namespace.yaml` | Creates the movie-analytics namespace |
| `configmap.yaml` | Application configuration and secrets |
| `kafka.yaml` | Kafka, Zookeeper, and Schema Registry |
| `minio.yaml` | MinIO (S3-compatible storage) with bucket setup |
| `mongodb.yaml` | MongoDB with initialization scripts |
| `spark.yaml` | Spark master, workers, and history server |
| `applications.yaml` | API server, data ingestion, and streaming jobs |
| `monitoring.yaml` | Prometheus and Grafana for monitoring |
| `visualization.yaml` | Apache Superset for dashboards |
| `deploy.sh` | Automated deployment script |

## 🚀 Quick Start

### Prerequisites

1. **Kubernetes Cluster**: Ensure you have a running Kubernetes cluster
2. **kubectl**: Install and configure kubectl to connect to your cluster
3. **Storage Class**: Ensure a default storage class is available for persistent volumes

### Deploy the Complete Stack

```bash
# Make the script executable
chmod +x deploy.sh

# Deploy everything
./deploy.sh deploy

# Or for a fresh deployment (cleans up first)
./deploy.sh redeploy
```

### Manual Deployment

If you prefer to deploy components manually:

```bash
# 1. Create namespace and configuration
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml

# 2. Deploy infrastructure (order matters)
kubectl apply -f kafka.yaml
kubectl apply -f minio.yaml
kubectl apply -f mongodb.yaml
kubectl apply -f spark.yaml

# 3. Deploy applications
kubectl apply -f applications.yaml

# 4. Deploy monitoring and visualization
kubectl apply -f monitoring.yaml
kubectl apply -f visualization.yaml
```

## 🔧 Configuration

### Required Secrets

Before deploying, update the secrets in `configmap.yaml`:

```yaml
stringData:
  TMDB_API_KEY: "your_actual_tmdb_api_key_here"
  MINIO_ACCESS_KEY: "your_minio_access_key"
  MINIO_SECRET_KEY: "your_minio_secret_key"
  # ... other secrets
```

### Environment Variables

Key configuration parameters in `configmap.yaml`:

- `TMDB_BASE_URL`: The Movie Database API endpoint
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `MINIO_ENDPOINT`: MinIO storage endpoint
- `MONGODB_DATABASE`: MongoDB database name
- `SPARK_MASTER_URL`: Spark cluster master URL

## 🏗️ Architecture Components

### Infrastructure Layer

- **Kafka Cluster**: Message streaming with Zookeeper
- **MinIO**: S3-compatible object storage (Bronze/Silver/Gold layers)
- **MongoDB**: NoSQL database for serving layer
- **Spark Cluster**: Distributed processing engine

### Application Layer

- **Data Ingestion**: TMDB API data extraction service
- **Streaming Processing**: Real-time data transformation
- **REST API**: FastAPI service for data access
- **Scheduled Jobs**: Periodic data extraction via CronJobs

### Monitoring & Visualization

- **Prometheus**: Metrics collection and alerting
- **Grafana**: Monitoring dashboards
- **Apache Superset**: Business intelligence dashboards

## 📊 Access Information

### Web Interfaces

After deployment, access these services:

| Service | Port | Description |
|---------|------|-------------|
| Grafana | 3000 | Monitoring dashboards (admin/admin) |
| Superset | 8088 | BI dashboards (admin/admin) |
| Movie API | 8000 | REST API endpoints |
| Spark Master UI | 8080 | Spark cluster management |
| MinIO Console | 9001 | Object storage management |

### Port Forwarding (Local Access)

```bash
# Access Grafana locally
kubectl port-forward -n movie-analytics service/grafana-service 3000:3000

# Access Superset locally
kubectl port-forward -n movie-analytics service/superset-service 8088:8088

# Access Movie API locally
kubectl port-forward -n movie-analytics service/movie-api-service 8000:8000

# Access Spark Master UI
kubectl port-forward -n movie-analytics service/spark-master-service 8080:8080
```

## 🔍 Monitoring & Troubleshooting

### Check Deployment Status

```bash
# View all pods
kubectl get pods -n movie-analytics

# Check services
kubectl get services -n movie-analytics

# View persistent volumes
kubectl get pvc -n movie-analytics

# Check jobs and cronjobs
kubectl get jobs,cronjobs -n movie-analytics
```

### View Logs

```bash
# Application logs
kubectl logs -n movie-analytics -l app=movie-api
kubectl logs -n movie-analytics -l app=data-ingestion

# Infrastructure logs
kubectl logs -n movie-analytics -l app=kafka
kubectl logs -n movie-analytics -l app=mongodb
kubectl logs -n movie-analytics -l app=spark-master

# Follow logs in real-time
kubectl logs -n movie-analytics -l app=movie-api -f
```

### Debug Common Issues

#### Pods Stuck in Pending
```bash
# Check node resources
kubectl describe nodes

# Check persistent volume claims
kubectl describe pvc -n movie-analytics
```

#### Application Startup Issues
```bash
# Check configuration
kubectl describe configmap movie-analytics-config -n movie-analytics

# Check secrets
kubectl describe secret movie-analytics-secrets -n movie-analytics

# View detailed pod status
kubectl describe pod <pod-name> -n movie-analytics
```

#### Network Connectivity Issues
```bash
# Test internal service connectivity
kubectl exec -it <pod-name> -n movie-analytics -- nslookup kafka-service
kubectl exec -it <pod-name> -n movie-analytics -- curl http://movie-api-service:8000/health
```

## 📈 Scaling

### Scale Applications

```bash
# Scale API servers
kubectl scale -n movie-analytics deployment/movie-api --replicas=3

# Scale Spark workers
kubectl scale -n movie-analytics deployment/spark-worker --replicas=4

# Scale data ingestion
kubectl scale -n movie-analytics deployment/data-ingestion --replicas=2
```

### Resource Management

Monitor resource usage:

```bash
# Check resource usage
kubectl top pods -n movie-analytics
kubectl top nodes

# View resource requests/limits
kubectl describe pod <pod-name> -n movie-analytics
```

## 🧹 Cleanup

### Remove Complete Stack

```bash
# Using the deployment script
./deploy.sh clean

# Manual cleanup
kubectl delete namespace movie-analytics
```

### Remove Specific Components

```bash
# Remove only monitoring
kubectl delete -f monitoring.yaml

# Remove only visualization
kubectl delete -f visualization.yaml
```

## 🔄 Updates and Maintenance

### Rolling Updates

```bash
# Update application image
kubectl set image -n movie-analytics deployment/movie-api movie-api=movie-analytics/api:v2.0

# Check rollout status
kubectl rollout status -n movie-analytics deployment/movie-api

# Rollback if needed
kubectl rollout undo -n movie-analytics deployment/movie-api
```

### Configuration Updates

```bash
# Update configuration
kubectl apply -f configmap.yaml

# Restart pods to pick up new config
kubectl rollout restart -n movie-analytics deployment/movie-api
```

## 🏆 Production Considerations

### Security
- Use proper RBAC configurations
- Implement network policies
- Use external secret management (e.g., Vault)
- Enable TLS for all communications

### High Availability
- Deploy across multiple nodes/zones
- Use anti-affinity rules for critical components
- Implement proper backup strategies

### Performance
- Monitor resource usage and scale accordingly
- Optimize persistent volume performance
- Use appropriate storage classes for workloads

### Backup Strategy
- Regular MongoDB backups
- MinIO data replication
- Configuration backup (configmaps, secrets)

This Kubernetes deployment provides a production-ready foundation for the Movie Analytics Pipeline with proper monitoring, scaling, and maintenance capabilities.