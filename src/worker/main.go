package main

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s-practice/batch"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Worker struct {
	id      string
	db      *gorm.DB
	redis   *redis.Client
	log     *logrus.Logger
	workers int
}

type ProcessingJob struct {
	JobID     string    `json:"job_id"`
	StartID   int64     `json:"start_id"`
	EndID     int64     `json:"end_id"`
	BatchSize int       `json:"batch_size"`
	JobType   string    `json:"job_type"`
	CreatedAt time.Time `json:"created_at"`
}

func NewWorker() *Worker {
	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

	// Worker ID (ใช้ hostname หรือ random)
	workerID := getEnv("WORKER_ID", "worker-"+fmt.Sprintf("%d", time.Now().Unix()))

	// Database connection
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_USER", "postgres"),
		getEnv("DB_PASSWORD", "postgres"),
		getEnv("DB_NAME", "postgres"),
		getEnv("DB_PORT", "5432"))

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	// Redis connection
	rdb := redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       0,
	})

	// Test Redis connection
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}

	// Number of concurrent workers per pod
	workers := 4
	if w := getEnv("CONCURRENT_WORKERS", ""); w != "" {
		fmt.Sscanf(w, "%d", &workers)
	}

	return &Worker{
		id:      workerID,
		db:      db,
		redis:   rdb,
		log:     log,
		workers: workers,
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.log.Infof("Starting worker %s with %d concurrent workers", w.id, w.workers)

	var wg sync.WaitGroup
	jobQueue := "processing_jobs"

	// Start multiple goroutines to process jobs concurrently
	for i := 0; i < w.workers; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			w.processJobs(ctx, jobQueue, workerNum)
		}(i)
	}

	wg.Wait()
	w.log.Info("All workers stopped")
}

func (w *Worker) processJobs(ctx context.Context, queueName string, workerNum int) {
	workerLog := w.log.WithFields(logrus.Fields{
		"worker_id":  w.id,
		"worker_num": workerNum,
	})

	for {
		select {
		case <-ctx.Done():
			workerLog.Info("Worker stopping due to context cancellation")
			return
		default:
			// BRPOP จะ block จนกว่าจะมี job ใน queue หรือ timeout
			result, err := w.redis.BRPop(ctx, 5*time.Second, queueName).Result()
			if err != nil {
				if err != redis.Nil {
					workerLog.Error("Failed to pop job from queue:", err)
				}
				continue
			}

			if len(result) < 2 {
				continue
			}

			jobData := result[1]
			if err := w.processJob(ctx, jobData, workerLog); err != nil {
				workerLog.Error("Failed to process job:", err)
			}
		}
	}
}

func (w *Worker) processJob(ctx context.Context, jobData string, log *logrus.Entry) error {
	var job ProcessingJob
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return fmt.Errorf("failed to unmarshal job: %w", err)
	}

	startTime := time.Now()
	log = log.WithField("job_id", job.JobID)
	log.Infof("Processing job: records %d-%d", job.StartID, job.EndID)

	// ประมวลผลข้อมูลจริง (ตัวอย่าง: อัปเดต batch_status)
	if err := w.processDataBatch(job.StartID, job.EndID); err != nil {
		log.Error("Failed to process data batch:", err)
		return err
	}

	duration := time.Since(startTime)
	recordsProcessed := job.EndID - job.StartID + 1
	rps := float64(recordsProcessed) / duration.Seconds()

	log.WithFields(logrus.Fields{
		"duration": duration,
		"records":  recordsProcessed,
		"rps":      rps,
	}).Info("Job completed successfully")

	return nil
}

func (w *Worker) processDataBatch(startID, endID int64) error {
	// ตัวอย่างการประมวลผล: อัปเดต batch_status จาก 'pending' เป็น 'processed'
	// และคำนวณ salary bonus
	result := w.db.Model(&batch.User{}).
		Where("id BETWEEN ? AND ? AND batch_status = ?", startID, endID, "pending").
		Updates(map[string]interface{}{
			"batch_status": "processed",
			"updated_at":   time.Now(),
		})

	if result.Error != nil {
		return fmt.Errorf("failed to update batch: %w", result.Error)
	}

	// บันทึก progress
	if result.RowsAffected > 0 {
		// อัปเดต batch job progress (ถ้าต้องการ)
		w.updateBatchProgress(int(result.RowsAffected))
	}

	return nil
}

func (w *Worker) updateBatchProgress(processedCount int) {
	// อัปเดต processed_records ใน batch_jobs table
	w.db.Model(&batch.BatchJob{}).
		Where("status = ?", "processing").
		UpdateColumn("processed_records", gorm.Expr("processed_records + ?", processedCount))
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	worker := NewWorker()

	go func() {
		<-sigChan
		logrus.Info("Received shutdown signal, stopping worker...")
		cancel()
	}()

	worker.Start(ctx)
}
