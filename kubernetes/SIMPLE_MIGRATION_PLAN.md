# Simple Kubernetes Migration Plan

## 🎯 Goal
Move your 28-service Docker Compose stack to Kubernetes with minimal learning curve.

---

## � WHERE WILL THIS RUN?

### Your Windows Computer (Local Development)
**What runs here:**
- ✅ Kompose tool (converts docker-compose to Kubernetes YAML)
- ✅ kubectl commands (to control Kubernetes)
- ✅ Docker Desktop with Kubernetes enabled (for testing only)

**What you do on your computer:**
```powershell
# Work directory: D:\Git\movie-data-analysis-pipeline
cd D:\Git\movie-data-analysis-pipeline

# Install tools
choco install kubernetes-cli kubernetes-kompose docker-desktop

# Convert docker-compose to Kubernetes
kompose convert -f docker-compose.yml -o kubernetes/generated/

# Control cloud cluster from your computer
kubectl apply -f kubernetes/generated/
```

### Cloud Server (Production - Where Services Actually Run)

**You need a Kubernetes cluster in the cloud:**

#### Option A: Digital Ocean (Recommended - Simplest)
- **What:** Managed Kubernetes cluster (3 servers running in DO data centers)
- **Cost:** $120-180/month
- **Setup time:** 10 minutes
- **You control it from your Windows PC using kubectl**

#### Option B: AWS EKS / Google GKE / Azure AKS
- **What:** Same as DO, different cloud provider
- **Cost:** Similar ($150-200/month)
- **Complexity:** More setup required

#### Option C: Your Own Servers (Not Recommended)
- **What:** Buy/rent 3 Linux servers, install Kubernetes yourself
- **Cost:** Variable
- **Complexity:** Very high, need Linux expertise

### ❌ DON'T Run Production on Your Windows Computer
**Why not:**
- Docker Desktop Kubernetes is for testing only
- Your computer needs to stay on 24/7
- Can't handle production load
- No redundancy if your computer crashes

---

## 🎬 SIMPLE DECISION TREE

**Q: "Where should I test first?"**
→ **A:** Your Windows computer with Docker Desktop Kubernetes (FREE, safe to break)

**Q: "Where should I run production?"**
→ **A:** Digital Ocean Kubernetes cluster (Paid, managed for you)

**Q: "Do I need to buy a server?"**
→ **A:** NO - Digital Ocean manages the servers, you just pay monthly

**Q: "What about my current Docker Compose setup?"**
→ **A:** Keep it! Test Kubernetes separately, switch when ready

---

## �📊 Current Stack Overview

**From docker-compose.yml:**
- **9 Serving Layer services** (MongoDB, Redis, API, Grafana, Prometheus, exporters)
- **7 Batch Layer services** (MinIO, PostgreSQL, Airflow, PySpark)
- **12 Speed Layer services** (Zookeeper, 3x Kafka, Schema Registry, Cassandra, streaming apps)

---

## 🚀 3-Phase Migration Strategy

### Phase 1: Serving Layer (Easiest - Start Here)
**Time: 2-3 hours | Risk: Low**

Services to migrate:
- ✅ MongoDB + MongoDB Express
- ✅ Redis
- ✅ FastAPI (serving-api)
- ✅ Prometheus + Grafana
- ✅ Exporters (MongoDB & Redis)

**Why start here:**
- Stateful but simple (MongoDB, Redis)
- No complex orchestration
- Can test API immediately
- Quick wins build confidence

**Steps:**
```bash
# 1. Use Kompose to auto-convert (saves 80% of work)
choco install kubernetes-kompose
kompose convert -f docker-compose.yml -o kubernetes/serving/

# 2. Fix the generated files (just update image names & add storage)
# 3. Deploy
kubectl apply -f kubernetes/serving/
```

---

### Phase 2: Batch Layer (Medium Difficulty)
**Time: 3-4 hours | Risk: Medium**

Services to migrate:
- ✅ MinIO (S3 storage)
- ✅ PostgreSQL (Airflow metadata)
- ✅ Airflow webserver + scheduler
- ✅ PySpark runner

**Challenges:**
- Airflow needs persistent volumes for logs
- MinIO needs persistent storage for Bronze/Silver/Gold data
- Airflow init jobs need to run once

**Quick Solution:**
```bash
# Use Helm charts (pre-built Kubernetes packages)
helm repo add bitnami https://charts.bitnami.com/bitnami

# Install MinIO
helm install minio bitnami/minio -f kubernetes/batch/minio-values.yaml

# Install PostgreSQL
helm install postgres bitnami/postgresql -f kubernetes/batch/postgres-values.yaml

# Install Airflow (community chart)
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow -f kubernetes/batch/airflow-values.yaml
```

---

### Phase 3: Speed Layer (Highest Complexity)
**Time: 4-6 hours | Risk: High**

Services to migrate:
- ✅ Zookeeper + 3x Kafka brokers
- ✅ Schema Registry
- ✅ Cassandra
- ✅ 4x streaming applications

**Challenges:**
- Kafka cluster coordination
- Cassandra StatefulSet complexity
- Streaming jobs need checkpointing

**Quick Solution:**
```bash
# Use battle-tested Helm charts
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add strimzi https://strimzi.io/charts/

# Install Kafka cluster (Strimzi operator handles complexity)
helm install kafka-operator strimzi/strimzi-kafka-operator
kubectl apply -f kubernetes/speed/kafka-cluster.yaml  # Simple YAML

# Install Cassandra
helm install cassandra bitnami/cassandra -f kubernetes/speed/cassandra-values.yaml

# Deploy streaming apps (Kompose-generated + minor fixes)
kubectl apply -f kubernetes/speed/streaming-apps/
```

---

## 🛠️ Tool: Auto-Convert with Kompose

**Why Kompose:**
- Converts 90% of docker-compose.yml automatically
- Generates Kubernetes YAML files instantly
- You just fix the 10% edge cases

**Install & Use:**
```powershell
# Install
choco install kubernetes-kompose

# Convert entire docker-compose.yml
cd d:\Git\movie-data-analysis-pipeline
kompose convert -f docker-compose.yml -o kubernetes/generated/

# Creates:
# - Deployments (for stateless apps)
# - StatefulSets (for databases)
# - Services (networking)
# - PersistentVolumeClaims (storage)
# - ConfigMaps (environment variables)
```

**After Kompose:**
1. Check generated YAML files
2. Fix image pull policies (set to `IfNotPresent` or `Always`)
3. Add resource limits (CPU/memory)
4. Verify persistent volume sizes
5. Deploy: `kubectl apply -f kubernetes/generated/`

---

## 📝 Simple Workflow (Complete Example)

### Step 1: Prep Your Windows Computer
```powershell
# ON YOUR WINDOWS PC (D:\Git\movie-data-analysis-pipeline)

# Install tools on your computer
choco install kubernetes-cli kubernetes-kompose kubernetes-helm

# Enable Docker Desktop Kubernetes (for local testing)
# 1. Open Docker Desktop
# 2. Settings → Kubernetes → Enable Kubernetes
# 3. Wait 2-3 minutes for it to start

# Verify it works
kubectl get nodes
# Should show: docker-desktop   Ready   control-plane   ...
```

### Step 1B: Create Cloud Cluster (Production)

Choose your cloud provider:

#### Option A: Google Kubernetes Engine (GKE) - Recommended
```powershell
# STILL ON YOUR WINDOWS PC - Commands control cloud servers

# Install Google Cloud SDK
choco install gcloudsdk

# Login to Google Cloud (opens browser)
gcloud auth login

# Set your project (replace PROJECT_ID with your actual project ID)
gcloud config set project PROJECT_ID

# Create GKE cluster (3 servers, 4 vCPU each, US region)
gcloud container clusters create movie-cluster `
  --zone us-central1-a `
  --machine-type e2-standard-4 `
  --num-nodes 3 `
  --enable-autoscaling `
  --min-nodes 3 `
  --max-nodes 6 `
  --disk-size 100 `
  --disk-type pd-standard

# Connect your Windows PC to the cloud cluster
gcloud container clusters get-credentials movie-cluster --zone us-central1-a

# Verify - now kubectl controls the CLOUD cluster
kubectl get nodes
# Should show: gke-movie-cluster-xxx   Ready   <none>   ... (3 nodes)
```

**Cost Estimate:** ~$150-180/month
- 3× e2-standard-4 (4 vCPU, 16GB RAM): ~$120/month
- Disk storage: ~$30/month
- Load Balancer: ~$20/month

**Where are the 3 servers?**
- Physical location: Iowa, USA (Google Cloud us-central1 data center)
- You never SSH into them
- You control them from your Windows PC using `kubectl`

---

#### Option B: Digital Ocean Kubernetes (DOKS) - Simpler
```powershell
# Install Digital Ocean CLI
choco install doctl

# Login to Digital Ocean (opens browser)
doctl auth init

# Create cluster in Digital Ocean cloud (3 servers in NYC)
doctl kubernetes cluster create movie-cluster `
  --region nyc3 `
  --size s-4vcpu-8gb `
  --count 3 `
  --wait

# Connect your Windows PC to the cloud cluster
doctl kubernetes cluster kubeconfig save movie-cluster

# Verify - now kubectl controls the CLOUD cluster
kubectl get nodes
# Should show: pool-xxx-yyy   Ready   <none>   ... (3 nodes)
```

**Cost Estimate:** ~$180/month
- 3× 4vCPU/8GB droplets: ~$120/month
- Load Balancer: $12/month
- Storage: ~$45/month

**Where are the 3 servers?**
- Physical location: New York City (Digital Ocean data center)
- You never SSH into them
- You control them from your Windows PC using `kubectl`

---

#### Option C: AWS EKS - Enterprise Grade
```powershell
# Install AWS CLI
choco install awscli

# Configure AWS credentials
aws configure
# Enter: Access Key ID, Secret Access Key, Region (us-east-1), Output (json)

# Install eksctl
choco install eksctl

# Create EKS cluster (3 servers, 4 vCPU each)
eksctl create cluster `
  --name movie-cluster `
  --region us-east-1 `
  --nodegroup-name standard-workers `
  --node-type t3.xlarge `
  --nodes 3 `
  --nodes-min 3 `
  --nodes-max 6 `
  --managed

# Verify connection
kubectl get nodes
# Should show: ip-xxx.ec2.internal   Ready   <none>   ... (3 nodes)
```

**Cost Estimate:** ~$200-250/month
- EKS control plane: $73/month (fixed)
- 3× t3.xlarge (4 vCPU, 16GB): ~$150/month
- Storage (EBS): ~$30/month

**Where are the 3 servers?**
- Physical location: Virginia, USA (AWS us-east-1 data center)
- You never SSH into them
- You control them from your Windows PC using `kubectl`

### Step 2: Build Custom Docker Images First
```powershell
# ON YOUR WINDOWS PC (D:\Git\movie-data-analysis-pipeline)
cd D:\Git\movie-data-analysis-pipeline

# Build Airflow image (takes 3-5 minutes)
docker build -t movie-pipeline-airflow:latest -f layers/batch_layer/Dockerfile.airflow layers/batch_layer

# Build Speed Layer image (takes 2-3 minutes)
docker build -t movie-pipeline-speed-layer:latest -f layers/speed_layer/Dockerfile layers/speed_layer

# Build Serving Layer API image (takes 1-2 minutes)
docker build -t movie-pipeline-serving-api:latest -f layers/serving_layer/Dockerfile layers/serving_layer

# Verify images exist
docker images | Select-String "movie-pipeline"
```

### Step 3: Convert & Deploy (Automated)
```powershell
# Convert docker-compose to Kubernetes YAML (runs on your PC)
kompose convert -f docker-compose.yml -o kubernetes/auto-generated/

# Review generated files (on your PC)
ls kubernetes/auto-generated/

# Choose where to deploy:

# ═══════════════════════════════════════════════════════════
# OPTION A: Test on local Docker Desktop first (RECOMMENDED)
# ═══════════════════════════════════════════════════════════
kubectl config use-context docker-desktop  # Switch to local
kubectl create namespace movie-pipeline
kubectl apply -f kubernetes/auto-generated/ -n movie-pipeline

# Watch deployment (pods run on your PC)
kubectl get pods -n movie-pipeline -w
ALL COMMANDS RUN ON YOUR WINDOWS PC

# Check services are running (in cloud or local)
kubectl get pods -n movie-pipeline

# If using LOCAL (Docker Desktop):
kubectl port-forward -n movie-pipeline svc/serving-api 8000:8000
# Test: http://localhost:8000/api/v1/health

# If using CLOUD (Digital Ocean):
kubectl get svc -n movie-pipeline serving-api
# Get EXTERNAL-IP (takes 2-3 min to provision)
# Test: http://<EXTERNAL-IP>:8000/api/v1/health

# Check logs if issues (works for both local and cloud)netes/auto-generated/ -n movie-pipeline

# Watch deployment (pods run in Digital Ocean cloud)
kubectl get pods -n movie-pipeline -w
```

**What happens:**
- Your computer sends YAML files to the cloud cluster
- Cloud cluster downloads Docker images and starts containers
- Services run on 3 cloud servers (not your PC)
- You view status/logs from your Windows PC using kubectl

### Step 4: Verify
```powershell
# Check services are running
kubectl get pods -n movie-pipeline

# Get service URLs
kubectl get services -n movie-pipeline

# Port-forward to test locally
kubectl port-forward -n movie-pipeline svc/serving-api 8000:8000
# Test: http://localhost:8000/api/v1/health

# Check logs if issues
kubectl logs -n movie-pipeline -l app=serving-api --tail=50
```

---

## 📦 What You Need to Change After Kompose

### 1. Fix Image Pull Policy
```yaml
# In each deployment/statefulset YAML
spec:
  template:
    spec:
      containers:
      - name: serving-api
        image: movie-pipeline-serving-api:latest
        imagePullPolicy: IfNotPresent  # ← Add this line
```

### 2. Add Resource Limits (Optional but recommended)
```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "100m"
  limits:
    memory: "1Gi"
    cpu: "500m"
```

### 3. Fix Persistent Volumes (if using cloud)
```yaml
# In PersistentVolumeClaim YAML
spec:
  storageClassName: do-block-storage  # ← Change for Digital Ocean
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

---

## 🔥 Cheat Sheet: Common Commands

```powershell
# Deploy everything
kubectl apply -f kubernetes/ -n movie-pipeline

# Check status
kubectl get all -n movie-pipeline

# View logs
kubectl logs -n movie-pipeline deployment/serving-api --tail=100 -f

# Restart a service
kubectl rollout restart deployment/serving-api -n movie-pipeline

# Delete everything (reset)
kubectl delete namespace movie-pipeline

# Scale services
kubectl scale deployment serving-api -n movie-pipeline --replicas=3

# Get service URL
kubectl get svc -n movie-pipeline serving-api
```

---

## ⚠️ Critical Differences: Docker Compose vs Kubernetes

| Docker Compose | Kubernetes | Fix |
|----------------|------------|-----|
| `depends_on` | No built-in | Add init containers or readiness probes |
| Service names = hostnames | Service names = DNS | No change needed! |
| `restart: unless-stopped` | Default behavior | No change needed |
| `ports: - "8000:8000"` | Needs `Service` object | Kompose creates this |
| `volumes: ./app:/app` | Needs `PersistentVolume` | Use PVC or hostPath |
| `.env` files | `ConfigMap` or `Secret` | Create manually |

---Week 1: Test Locally (Your Windows Computer)
**Where:** Docker Desktop Kubernetes on your Windows PC
**Why:** Free, safe to break, learn without cost
**Steps:**
1. Enable Kubernetes in Docker Desktop
2. Convert docker-compose with Kompose
3. Deploy to local cluster: `kubectl apply -f kubernetes/generated/`
4. Fix errors, iterate, learn kubectl basics
5. Verify services work: `http://localhost:8000`

**Commands run on:** Your Windows PC
**Services run on:** Your Windows PC (Docker Desktop)
**Cost:** $0

### Week 2: Deploy to Cloud (Production)
**Where:** Digital Ocean Kubernetes cluster (3 servers in cloud)
**Why:** Production-ready, scalable, 24/7 uptime
**Steps:**
1. Create DO account, add billing
2. Run `doctl kubernetes cluster create` from your Windows PC
3. Switch context: `kubectl config use-context do-nyc3-movie-cluster`
4. Deploy same YAML: `kubectl apply -f kubernetes/generated/`
5. Get public IP, configure DNS

**Commands run on:** Your Windows PC
**Services run on:** 3 cloud servers in Digital Ocean (you never SSH into them)
**Cost:** ~$180/month

---

## 🖥️ Hardware Requirements

### Your Windows Computer
**Needs:**
- Windows 10/11
- 8GB RAM minimum (16GB recommended for local testing)
- Docker Desktop installed
- Internet connection
- PowerShell

**What it does:**
- Runs kubectl commands
- Runs Kompose tool
- Optionally runs Docker Desktop Kubernetes (for testing)

### Cloud Servers (Digital Ocean)
**Managed by Digital Ocean (not you):**
- 3 servers × 4 vCPU / 8GB RAM
- Located in NYC data center
- You pay monthly, they handle hardware/maintenance
- You control via kubectl from your Windows PC(use Kompose)
2. Phase 2: Batch Layer (use Helm charts)
3. Phase 3: Speed Layer (mix of Helm + Kompose)
4. Add monitoring & resource limits
5. Deploy to Digital Ocean

---

## 📚 Minimal Learning Resources

**You only need to learn:**
1. `kubectl apply` - Deploy stuff
2. `kubectl get pods` - Check status
3. `kubectl logs` - Debug issues
4. `kubectl port-forward` - Test services

**5-Minute Tutorials:**
- [kubectl Cheat Sheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/)
- [Kompose User Guide](https://kompose.io/user-guide/)
- [What is a Pod?](https://kubernetes.io/docs/concepts/workloads/pods/) (2 min read)

---

## 🆘 When You Get Stuck

```powershell
# Diagnosis toolkit
kubectl get events --sort-by='.lastTimestamp'  # What happened?
kubectl describe pod <pod-name>  # Why did it fail?
kubectl logs <pod-name> --previous  # Logs from crash
```

**Common Issues:**

| Issue | Symptom | Solution |
|-------|---------|----------|
| **ImagePullBackOff** | Can't find image | Build images locally: `docker build -t <image> .` |
| **CrashLoopBackOff** | Container crashes | Check logs: `kubectl logs <pod-name>` |
| **Pending** | Pod stuck | Not enough resources → Scale cluster or reduce replicas |
| **Error / Failed** | Won't start | Check events: `kubectl describe pod <pod-name>` |
| **ContainerCreating** (stuck) | Hangs forever | Usually volume mount issue → Check PVC status |

**Provider-specific issues:**

**GKE:**
```powershell
# Check cluster status
gcloud container clusters describe movie-cluster --zone us-central1-a

# Resize cluster if needed
gcloud container clusters resize movie-cluster --num-nodes 4 --zone us-central1-a

# View GKE logs
gcloud logging read "resource.type=k8s_cluster" --limit 50
```

**Digital Ocean:**
```powershell
# Check cluster status
doctl kubernetes cluster get movie-cluster

# Resize cluster
doctl kubernetes cluster node-pool update movie-cluster worker-pool --count 4
```

**AWS EKS:**
```powershell
# Check cluster status
eksctl get cluster movie-cluster

# Scale nodes
eksctl scale nodegroup --cluster=movie-cluster --name=standard-workers --nodes=4
```

---
### For Local Testing (Your Windows PC)
```powershell
# 1. Install tools
choco install kubernetes-kompose docker-desktop

# 2. Enable Kubernetes in Docker Desktop (Settings → Kubernetes → Enable)

# 3. Convert and deploy
cd D:\Git\movie-data-analysis-pipeline
kompose convert -f docker-compose.yml -o kubernetes\generated\
kubectl apply -f kubernetes\generated\serving-*.yaml  # Deploy only serving layer

# 4. Test API
kubectl port-forward svc/serving-api 8000:8000
# Open: http://localhost:8000/api/v1/health
```

### For Production (Google Cloud)
```powershell
# 1. Install Google Cloud SDK
choco install gcloudsdk

# 2. Create GKE cluster
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud container clusters create movie-cluster `
  --zone us-central1-a `
  --machine-type e2-standard-4 `
  --num-nodes 3

# 3. Build and push images to Google Container Registry
docker build -t gcr.io/YOUR_PROJECT_ID/movie-airflow:latest -f layers/batch_layer/Dockerfile.airflow layers/batch_layer
docker build -t gcr.io/YOUR_PROJECT_ID/movie-speed:latest -f layers/speed_layer/Dockerfile layers/speed_layer
docker build -t gcr.io/YOUR_PROJECT_ID/movie-api:latest -f layers/serving_layer/Dockerfile layers/serving_layer

gcloud auth configure-docker
docker push gcr.io/YOUR_PROJECT_ID/movie-airflow:latest
docker push gcr.io/YOUR_PROJECT_ID/movie-speed:latest
docker push gcr.io/YOUR_PROJECT_ID/movie-api:latest

# 4. Update image references in YAML files
# Edit kubernetes/generated/*.yaml files to use gcr.io/YOUR_PROJECT_ID/* images

# 5. Deploy
gcloud container clusters get-credentials movie-cluster --zone us-central1-a
kubectl apply -f kubernetes\generated\

# 6. Get external IP
kubectl get services
```

### Quick Start Guide by Cloud Provider

**Google Cloud (Recommended):**
- Sign up: https://console.cloud.google.com/ (Free $300 credit)
- Create project → Enable Kubernetes Engine API
- Follow "Production (Google Cloud)" steps above
- Total time: ~20 minutes

**Digital Ocean:**
- Sign up: https://cloud.digitalocean.com/
- Add billing method
- Run commands from "Option B: Digital Ocean Kubernetes (DOKS)"
- Total time: ~15 minutes

**AWS:**
- Sign up: https://aws.amazon.com/
- Create IAM user with admin access
- Follow "Option C: AWS EKS" steps
- Total time: ~30 minutes
- [ ] API health check works: `curl http://api-url/api/v1/health`
- [ ] Grafana shows metrics
- [ ] Airflow DAGs appear in web UI
- [ ] Kafka topics exist and streaming works

---

## 💰 Cost Estimate

| Provider | Configuration | Monthly Cost | Best For |
|----------|--------------|--------------|----------|
| **Docker Desktop** | Local testing | **FREE** | Learning, testing |
| **Google GKE** | 3× e2-standard-4 | **~$150-180** | Production (recommended) |
| **Digital Ocean** | 3× 4vCPU/8GB | **~$180** | Simplest setup |
| **AWS EKS** | 3× t3.xlarge | **~$200-250** | Enterprise features |

**Budget Options:**
- GKE: 2× e2-medium (2vCPU/4GB) = ~$60/month (dev only)
- Digital Ocean: 2× 2vCPU/4GB = ~$48/month (dev only)

**Why GKE is recommended:**
- Better pricing for same specs
- Excellent autoscaling
- Good free tier ($300 credit for 90 days)
- Easy to use
- Better performance than DO

---

## ✅ COMPLETED STEPS (December 21, 2025)

### Phase 1: Setup & Cluster Creation ✓
- [x] Installed Google Cloud SDK
- [x] Authenticated with Google Cloud (`gcloud auth login`)
- [x] Set project: `movie-analysis-pipeline`
- [x] Enabled Kubernetes Engine API
- [x] Created GKE cluster: `movie-cluster-mini`
  - 2 nodes × e2-standard-2 (4 vCPU, 16GB RAM)
  - Zone: us-central1-a
  - Cost: ~$48/month
- [x] Installed GKE auth plugin
- [x] Connected kubectl to cluster

### Phase 2: Docker Image Build ✓
- [x] Built `movie-pipeline-airflow:latest` (2.35GB)
- [x] Built `movie-pipeline-speed-layer:latest` (1.11GB)
- [x] Built `movie-pipeline-serving-api:latest` (591MB)

### Phase 3: Kubernetes Deployment ✓
- [x] Converted docker-compose to Kubernetes YAML using Kompose
- [x] Deployed all 28 services to GKE cluster
- [x] Verified cluster connectivity

### Phase 4: Image Registry ✓
- [x] Logged into Docker Hub (auphong2707)
- [x] Pushed all 3 custom images to Docker Hub
- [x] Updated Kubernetes deployments to use Docker Hub images
- [x] Reapplied configurations to cluster

### Current Status (13/26 pods running):
✅ **Running Successfully:**
- Batch Layer: Airflow scheduler, Airflow webserver, MinIO, PySpark runner
- Serving Layer: API, MongoDB, MongoDB exporter, Redis, Redis exporter
- Speed Layer: Cassandra, Kafka (3 brokers), Schema Registry, Zookeeper

🔄 **Pods Needing Attention (13):**
- CrashLoopBackOff (6): Postgres, Grafana, Mongo-Express, Prometheus, Cassandra-Mongo-Sync, Airflow-Init
- Error (5): Init pods and producers (likely dependency/config issues)
- ContainerCreating (1): Reddit sentiment stream

🎯 **Major Success:** Core infrastructure is operational! Kafka cluster, Cassandra, MongoDB, Redis, and custom applications (Airflow, API) are all running on Kubernetes.

---

## 🚦 Next Steps

### Immediate: Push Docker Images to GCR

```powershell
# Configure Docker for Google Container Registry
gcloud auth configure-docker

# Tag and push Airflow image
docker tag movie-pipeline-airflow:latest gcr.io/movie-analysis-pipeline/airflow:latest
docker push gcr.io/movie-analysis-pipeline/airflow:latest

# Tag and push Serving API
docker tag movie-pipeline-serving-api:latest gcr.io/movie-analysis-pipeline/serving-api:latest
docker push gcr.io/movie-analysis-pipeline/serving-api:latest

# Tag and push Speed Layer
docker tag movie-pipeline-speed-layer:latest gcr.io/movie-analysis-pipeline/speed-layer:latest
docker push gcr.io/movie-analysis-pipeline/speed-layer:latest
```

### Then: Update Kubernetes Deployments

Update image references in YAML files to use GCR:
- `movie-pipeline-airflow:latest` → `gcr.io/movie-analysis-pipeline/airflow:latest`
- `movie-pipeline-serving-api:latest` → `gcr.io/movie-analysis-pipeline/serving-api:latest`
- `movie-pipeline-speed-layer:latest` → `gcr.io/movie-analysis-pipeline/speed-layer:latest`

### Finally: Verify All Services

```powershell
# Check all pods are running
kubectl get pods

# Get external IPs
kubectl get services

# Test API endpoint
kubectl port-forward svc/serving-api 8000:8000
```

**Questions?** Check the [DIGITAL_OCEAN_DEPLOYMENT_GUIDE.md](DIGITAL_OCEAN_DEPLOYMENT_GUIDE.md) for detailed cloud setup.
