.PHONY: seed-schema seed-generate seed-import seed-all build-images deploy-k8s install-keda clean-k8s clean-keda

# Database seeding commands
seed-schema:
	go run src/seed/seed.go schema

seed-generate:
	go run src/seed/seed.go generate

seed-import:
	go run src/seed/seed.go import

seed-all:
	go run src/seed/seed.go all

build-images:
	docker build -t batch-processor:latest .
	docker build -t batch-scheduler:latest .

install-keda:
	helm install keda kedacore/keda --namespace keda --create-namespace

clean-keda:
	kubectl delete scaledjobs --all
	kubectl delete scaledobjects --all
	kubectl delete crd cloudeventsources.eventing.keda.sh
	kubectl delete crd scaledjobs.keda.sh
	kubectl delete crd scaledobjects.keda.sh
	kubectl delete crd triggerauthentications.keda.sh
	kubectl delete crd clustertriggerauthentications.keda.sh
	kubectl delete namespace keda --ignore-not-found=true
	kubectl delete validatingwebhookconfiguration keda-admission --ignore-not-found=true
	kubectl delete mutatingwebhookconfiguration keda-admission --ignore-not-found=true
	kubectl delete clusterrole keda-operator --ignore-not-found=true
	kubectl delete clusterrolebinding keda-operator --ignore-not-found=true
	kubectl delete apiservice v1beta1.external.metrics.k8s.io --ignore-not-found=true
	kubectl get crd | grep keda | awk '{print $1}' | xargs -r kubectl delete crd
	kubectl delete namespace keda --ignore-not-found=true

test-local:
	docker-compose up -d postgres
	docker-compose --profile tools run --rm batch-scheduler
	docker-compose up batch-worker

deploy-k8s:
	kubectl apply -f k8s/secret.yml
	kubectl apply -f k8s/scalejob.yml
	kubectl apply -f k8s/cronjob.yml

clean-k8s:
	kubectl delete -f k8s/secret.yml
	kubectl delete -f k8s/scalejob.yml
	kubectl delete -f k8s/cronjob.yml