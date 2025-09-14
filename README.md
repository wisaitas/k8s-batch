# Kubernetes Batch Processing with KEDA Auto-scaling

A Proof of Concept (POC) for scalable batch processing on Kubernetes using KEDA (Kubernetes Event-driven Autoscaling) with PostgreSQL as the work queue.

## 🎯 Project Overview

This project demonstrates how to build a production-ready batch processing system that can:

- **Auto-scale workers** based on queue depth using KEDA
- **Process millions of records** efficiently with parallel workers
- **Handle failures gracefully** with retry mechanisms and job tracking
- **Monitor progress** with batch job status tracking
- **Claim-based processing** to prevent duplicate work across workers

## 🏗️ Architecture

```mermaid
graph TB
    subgraph "Data Generation"
        Seed[Seed Tool<br/>generates CSV files]
        CSV[CSV Files<br/>chunked data]
    end

    subgraph "Database Layer"
        PG[(PostgreSQL<br/>Queue & Storage)]
        Users[Users Table<br/>pending/processing/completed/failed]
        BatchJobs[Batch Jobs Table<br/>tracking & monitoring]
    end

    subgraph "Kubernetes Cluster"
        KEDA[KEDA Scaler<br/>monitors queue depth]
        Auth[TriggerAuthentication<br/>DB credentials]
        ScaledJob[ScaledJob<br/>manages worker pods]

        subgraph "Worker Pods"
            W1[Worker Pod 1]
            W2[Worker Pod 2]
            W3[Worker Pod N...]
        end
    end

    subgraph "Processing Flow"
        Claim[Claim Records<br/>SELECT...FOR UPDATE SKIP LOCKED]
        Process[Process Business Logic<br/>with retries]
        Update[Update Status<br/>completed/failed]
    end

    Seed --> CSV
    CSV --> PG
    PG --> Users
    PG --> BatchJobs

    KEDA --> PG
    KEDA --> ScaledJob
    Auth --> KEDA
    ScaledJob --> W1
    ScaledJob --> W2
    ScaledJob --> W3

    W1 --> Claim
    W2 --> Claim
    W3 --> Claim

    Claim --> Process
    Process --> Update
    Update --> PG

    PG -.->|Query: COUNT pending/failed| KEDA
```

นี่คือ README ที่ครอบคลุมสำหรับโปรเจกต์ K8s Batch Processing ของคุณ โดยมี:

- **ภาพรวมโปรเจกต์** และความสามารถหลัก
- **Architecture diagram** ด้วย Mermaid ที่แสดงการทำงานของระบบ
- **วิธีการติดตั้งและใช้งาน** ทั้งในโหมด development และ production
- **การ monitoring และ troubleshooting**
- **การปรับแต่ง performance**
- **ข้อมูลสำหรับการนำไปใช้จริง**

README นี้จะช่วยให้ผู้ใช้เข้าใจโปรเจกต์และสามารถนำไปใช้งานได้อย่างมีประสิทธิภาพ

## 🚀 Features

### Core Features

- **KEDA Auto-scaling**: Automatically scales workers based on pending record count
- **Claim-based Processing**: Uses `SELECT...FOR UPDATE SKIP LOCKED` to prevent race conditions
- **Batch Job Tracking**: Full visibility into processing progress and failures
- **Retry Logic**: Built-in retry mechanism for failed records
- **Graceful Shutdown**: Proper signal handling and cleanup

### Production-Ready Features

- **Resource Management**: CPU/Memory limits and requests
- **Health Monitoring**: Batch job status tracking
- **Error Handling**: Failed record tracking with error messages
- **Configurable**: Environment-based configuration
- **Docker Multi-stage**: Optimized container images

## 🛠️ Prerequisites

- Docker & Docker Compose
- Kubernetes cluster (local: minikube, k3s, or cloud: EKS, GKE, AKS)
- KEDA installed on your cluster
- Go 1.24+ (for development)

### Install KEDA

```bash
# Using Helm
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda-system --create-namespace

# Or using kubectl
kubectl apply -f https://github.com/kedacore/keda/releases/download/v2.12.0/keda-2.12.0.yaml
```

## 🔧 Local Development Setup

### 1. Start PostgreSQL

```bash
docker-compose up -d postgres
```

### 2. Generate Test Data

```bash
cd src/seed
go run seed.go schema  # Create database schema
go run seed.go generate  # Generate 50M test records
go run seed.go import   # Import to database

# Or do everything at once
go run seed.go all
```

### 3. Build Worker Image

```bash
cd src/worker
docker build -t batch-worker:latest .
```

### 4. Test Worker Locally

```bash
export PG_DSN="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
export CLAIM_SIZE=1000
export WORKERS=4
go run worker.go
```

## ☸️ Kubernetes Deployment

### 1. Deploy Database Secrets

```bash
kubectl apply -f k8s/pg-secret.yml
```

### 2. Deploy KEDA Configuration

```bash
kubectl apply -f k8s/trigger-auth.yml
kubectl apply -f k8s/scalejob.yml
```

### 3. Monitor Scaling

```bash
# Watch KEDA scaling
kubectl get scaledjob users-batch -w

# Watch job pods
kubectl get pods -l job-name -w

# View logs
kubectl logs -l job-name --tail=100 -f
```

## 📊 Monitoring & Observability

### Database Queries

```sql
-- Check queue depth (this is what KEDA monitors)
SELECT COUNT(1) FROM users WHERE batch_status IN ('pending','failed');

-- Check processing status
SELECT batch_status, COUNT(*) FROM users GROUP BY batch_status;

-- View batch job progress
SELECT * FROM batch_jobs ORDER BY started_at DESC LIMIT 10;

-- Active workers
SELECT DISTINCT batch_id, batch_name FROM batch_jobs WHERE status = 'running';
```

### Kubernetes Monitoring

```bash
# KEDA metrics
kubectl get hpa -n keda-system

# Job history
kubectl get jobs --sort-by=.metadata.creationTimestamp

# Resource usage
kubectl top pods -l job-name
```

## ⚙️ Configuration

### Environment Variables

| Variable           | Default        | Description                          |
| ------------------ | -------------- | ------------------------------------ |
| `PG_DSN`           | -              | PostgreSQL connection string         |
| `CLAIM_SIZE`       | `20000`        | Number of records to claim per batch |
| `WORKERS`          | `8`            | Number of concurrent workers per pod |
| `CLAIM_TIMEOUT_S`  | `30s`          | Timeout for claiming records         |
| `SHUTDOWN_GRACE_S` | `30s`          | Graceful shutdown timeout            |
| `POD_NAME`         | auto-generated | Pod identifier for tracking          |

### KEDA Scaling Configuration

```yaml
# k8s/scalejob.yml
spec:
  pollingInterval: 30 # Check queue every 30 seconds
  maxReplicaCount: 5 # Maximum worker pods
  triggers:
    - type: postgresql
      metadata:
        query: "SELECT COUNT(1) FROM users WHERE batch_status IN ('pending','failed')"
        targetQueryValue: "20000" # Scale up when backlog > 20K per pod
```

## 🚢 Production Deployment

### 1. Customize Configuration

```bash
# Update database connection in k8s/pg-secret.yml
# Adjust scaling parameters in k8s/scalejob.yml
# Set appropriate resource limits
```

### 2. Build & Push Images

```bash
# Build for your registry
docker build -t your-registry/batch-worker:v1.0.0 src/worker/

# Push to registry
docker push your-registry/batch-worker:v1.0.0

# Update image in k8s/scalejob.yml
```

### 3. Deploy to Production

```bash
# Apply all configs
kubectl apply -f k8s/

# Verify deployment
kubectl get scaledjob
kubectl describe scaledjob users-batch
```

## 🔍 Troubleshooting

### Common Issues

**KEDA not scaling:**

```bash
# Check KEDA logs
kubectl logs -n keda-system deployment/keda-operator

# Verify trigger authentication
kubectl describe triggerauthentication pgconn

# Test database connection
kubectl run -it --rm debug --image=postgres:17 -- psql "postgres://postgres:postgres@host.docker.internal:5432/postgres"
```

**Workers not processing:**

```bash
# Check worker logs
kubectl logs -l job-name

# Verify database connectivity
kubectl exec -it deployment/postgres -- psql -U postgres -c "SELECT COUNT(*) FROM users WHERE batch_status = 'pending';"

# Check resource constraints
kubectl describe pod <worker-pod-name>
```

**Performance issues:**

```bash
# Monitor resource usage
kubectl top pods

# Check database connections
kubectl exec -it deployment/postgres -- psql -U postgres -c "SELECT count(*) FROM pg_stat_activity;"

# Adjust CLAIM_SIZE and WORKERS based on workload
```

## 📈 Performance Tuning

### Database Optimization

```sql
-- Add indexes for better performance
CREATE INDEX CONCURRENTLY idx_users_batch_status_id ON users(batch_status, id);

-- Monitor slow queries
SELECT query, mean_time, calls FROM pg_stat_statements ORDER BY mean_time DESC;
```

### Scaling Parameters

- **Increase `CLAIM_SIZE`** for larger batches (reduces DB overhead)
- **Increase `WORKERS`** for CPU-intensive processing
- **Adjust `targetQueryValue`** to control scaling sensitivity
- **Tune `pollingInterval`** for responsiveness vs overhead

## 🎓 Learning Outcomes

This POC demonstrates:

1. **Event-driven Autoscaling** with KEDA
2. **Database as Queue** pattern with PostgreSQL
3. **Claim-based Processing** for distributed systems
4. **Kubernetes Job Management** and lifecycle
5. **Production-ready Patterns** for batch processing

## 📝 License

This project is for educational and demonstration purposes.

## 🤝 Contributing

Feel free to submit issues and enhancement requests!
