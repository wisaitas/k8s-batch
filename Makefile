.PHONY: build deploy-redis deploy-publisher deploy-workers scale-up scale-down clean

# Build Docker image
build:
	docker build -t batch-job:latest -f batch/Dockerfile .

# Deploy Redis
deploy-redis:
	kubectl apply -f k8s/redis/deployment.yml

# Deploy job publisher
deploy-publisher:
	kubectl apply -f k8s/batch/publisher-job.yml

# Deploy workers with HPA
deploy-workers:
	kubectl apply -f k8s/batch/worker-deployment.yml
	kubectl apply -f k8s/batch/hpa.yml

# Scale up workers manually (if needed)
scale-up:
	kubectl scale deployment batch-worker --replicas=20

# Scale down workers
scale-down:
	kubectl scale deployment batch-worker --replicas=2

# Monitor queue length
monitor-queue:
	kubectl exec -it deployment/redis -- redis-cli llen processing_jobs

# Monitor worker status
monitor-workers:
	kubectl get pods -l app=batch-worker
	kubectl top pods -l app=batch-worker

# Clean up
clean:
	kubectl delete -f k8s/batch/ --ignore-not-found
	kubectl delete -f k8s/redis/ --ignore-not-found

# Full deployment
deploy-all: build deploy-redis deploy-workers deploy-publisher

# Check job progress
check-progress:
	@echo "Queue length:"
	@kubectl exec -it deployment/redis -- redis-cli llen processing_jobs
	@echo "\nWorker pods:"
	@kubectl get pods -l app=batch-worker
	@echo "\nHPA status:"
	@kubectl get hpa batch-worker-hpa