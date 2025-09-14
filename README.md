# install dependency python
pip3 install psycopg2-binary
pip3 install pandas

# start db
docker-compose up -d postgres

# seed 
make seed-schema
make seed-generate
make seed-import
make seed-all

# deploy
make build-images
make deploy-k8s

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CronJob       │    │  KEDA ScaledJob  │    │   PostgreSQL    │
│   (Scheduler)   │───▶│   (Workers)      │◀──▶│   (Data)        │
│   - Create      │    │   - Process      │    │   - 100M users  │
│   - Monitor     │    │   - Scale        │    │   - Batch jobs  │
└─────────────────┘    └──────────────────┘    └─────────────────┘