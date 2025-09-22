# Deployment script for Movie Analytics Pipeline
#!/bin/bash

set -e  # Exit on any error

echo "🎬 Movie Analytics Pipeline - Kubernetes Deployment"
echo "=================================================="

# Configuration
NAMESPACE="movie-analytics"
KUBECTL_TIMEOUT="300s"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed or not in PATH"
        exit 1
    fi
    print_success "kubectl is available"
}

# Function to check if cluster is accessible
check_cluster() {
    if ! kubectl cluster-info &> /dev/null; then
        print_error "Cannot connect to Kubernetes cluster"
        exit 1
    fi
    print_success "Kubernetes cluster is accessible"
}

# Function to create namespace
create_namespace() {
    print_status "Creating namespace: $NAMESPACE"
    kubectl apply -f namespace.yaml
    print_success "Namespace created/updated"
}

# Function to apply configuration
apply_config() {
    print_status "Applying configuration and secrets"
    kubectl apply -f configmap.yaml
    print_success "Configuration applied"
}

# Function to deploy infrastructure components
deploy_infrastructure() {
    print_status "Deploying infrastructure components..."
    
    # Deploy Kafka
    print_status "Deploying Kafka and Zookeeper"
    kubectl apply -f kafka.yaml
    
    # Deploy MinIO
    print_status "Deploying MinIO (S3-compatible storage)"
    kubectl apply -f minio.yaml
    
    # Deploy MongoDB
    print_status "Deploying MongoDB"
    kubectl apply -f mongodb.yaml
    
    # Deploy Spark
    print_status "Deploying Spark cluster"
    kubectl apply -f spark.yaml
    
    print_success "Infrastructure components deployed"
}

# Function to deploy applications
deploy_applications() {
    print_status "Deploying application components..."
    kubectl apply -f applications.yaml
    print_success "Application components deployed"
}

# Function to deploy monitoring
deploy_monitoring() {
    print_status "Deploying monitoring stack (Prometheus, Grafana)"
    kubectl apply -f monitoring.yaml
    print_success "Monitoring stack deployed"
}

# Function to deploy visualization
deploy_visualization() {
    print_status "Deploying visualization stack (Superset)"
    kubectl apply -f visualization.yaml
    print_success "Visualization stack deployed"
}

# Function to wait for deployments
wait_for_deployments() {
    print_status "Waiting for deployments to be ready..."
    
    # Core infrastructure
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/zookeeper -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/kafka -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/minio -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT statefulset/mongodb -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/spark-master -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/spark-worker -n $NAMESPACE
    
    # Applications
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/movie-api -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/data-ingestion -n $NAMESPACE
    
    # Monitoring
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/prometheus -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/grafana -n $NAMESPACE
    
    # Visualization
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/superset -n $NAMESPACE
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/superset-db -n $NAMESPACE
    
    print_success "All deployments are ready"
}

# Function to run initialization jobs
run_init_jobs() {
    print_status "Running initialization jobs..."
    
    # Wait for MinIO bucket creation
    kubectl wait --for=condition=complete --timeout=$KUBECTL_TIMEOUT job/minio-bucket-setup -n $NAMESPACE
    print_success "MinIO buckets created"
    
    # Wait for MongoDB setup
    kubectl wait --for=condition=complete --timeout=$KUBECTL_TIMEOUT job/mongodb-setup -n $NAMESPACE
    print_success "MongoDB initialized"
    
    # Wait for Superset admin creation
    kubectl wait --for=condition=complete --timeout=$KUBECTL_TIMEOUT job/superset-init-admin -n $NAMESPACE
    print_success "Superset admin user created"
}

# Function to display access information
show_access_info() {
    print_success "Deployment completed successfully!"
    echo ""
    echo "🌐 Access Information:"
    echo "===================="
    
    # Get LoadBalancer IPs/Ports
    GRAFANA_IP=$(kubectl get service grafana-service -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")
    SUPERSET_IP=$(kubectl get service superset-service -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")
    
    if [ "$GRAFANA_IP" = "pending" ] || [ "$GRAFANA_IP" = "" ]; then
        GRAFANA_PORT=$(kubectl get service grafana-service -n $NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "3000")
        echo "📊 Grafana (Monitoring): http://localhost:$GRAFANA_PORT (port-forward required)"
    else
        echo "📊 Grafana (Monitoring): http://$GRAFANA_IP:3000"
    fi
    
    if [ "$SUPERSET_IP" = "pending" ] || [ "$SUPERSET_IP" = "" ]; then
        SUPERSET_PORT=$(kubectl get service superset-service -n $NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "8088")
        echo "📈 Superset (Visualization): http://localhost:$SUPERSET_PORT (port-forward required)"
    else
        echo "📈 Superset (Visualization): http://$SUPERSET_IP:8088"
    fi
    
    echo ""
    echo "🔑 Default Credentials:"
    echo "====================="
    echo "Grafana: admin / admin"
    echo "Superset: admin / admin"
    echo ""
    echo "🚀 Port Forward Commands (if LoadBalancer not available):"
    echo "======================================================="
    echo "kubectl port-forward -n $NAMESPACE service/grafana-service 3000:3000"
    echo "kubectl port-forward -n $NAMESPACE service/superset-service 8088:8088"
    echo "kubectl port-forward -n $NAMESPACE service/movie-api-service 8000:8000"
    echo ""
    echo "📋 Useful Commands:"
    echo "=================="
    echo "View all pods: kubectl get pods -n $NAMESPACE"
    echo "View services: kubectl get services -n $NAMESPACE"
    echo "View logs: kubectl logs -n $NAMESPACE -l app=<app-name>"
    echo "Scale deployment: kubectl scale -n $NAMESPACE deployment/<name> --replicas=<count>"
}

# Function to clean up (optional)
cleanup() {
    print_warning "Cleaning up existing deployment..."
    kubectl delete namespace $NAMESPACE --ignore-not-found=true
    print_success "Cleanup completed"
}

# Main execution
main() {
    echo "Starting deployment process..."
    
    # Check prerequisites
    check_kubectl
    check_cluster
    
    # Handle command line arguments
    case "${1:-deploy}" in
        "clean")
            cleanup
            ;;
        "deploy")
            create_namespace
            apply_config
            deploy_infrastructure
            deploy_applications
            deploy_monitoring
            deploy_visualization
            wait_for_deployments
            run_init_jobs
            show_access_info
            ;;
        "redeploy")
            cleanup
            sleep 10
            create_namespace
            apply_config
            deploy_infrastructure
            deploy_applications
            deploy_monitoring
            deploy_visualization
            wait_for_deployments
            run_init_jobs
            show_access_info
            ;;
        *)
            echo "Usage: $0 [deploy|clean|redeploy]"
            echo "  deploy   - Deploy the complete stack (default)"
            echo "  clean    - Remove all components"
            echo "  redeploy - Clean and deploy"
            exit 1
            ;;
    esac
}

# Execute main function
main "$@"