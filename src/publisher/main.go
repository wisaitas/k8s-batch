package main

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s-practice/batch"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type JobPublisher struct {
	db    *gorm.DB
	redis *redis.Client
	log   *logrus.Logger
}

type ProcessingJob struct {
	JobID     string    `json:"job_id"`
	StartID   int64     `json:"start_id"`
	EndID     int64     `json:"end_id"`
	BatchSize int       `json:"batch_size"`
	JobType   string    `json:"job_type"`
	CreatedAt time.Time `json:"created_at"`
}

func NewJobPublisher() *JobPublisher {
	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

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

	return &JobPublisher{
		db:    db,
		redis: rdb,
		log:   log,
	}
}

func (jp *JobPublisher) PublishJobs(ctx context.Context) error {
	// นับจำนวน records ทั้งหมด
	var totalRecords int64
	if err := jp.db.Model(&batch.User{}).Count(&totalRecords).Error; err != nil {
		return fmt.Errorf("failed to count records: %w", err)
	}

	jp.log.Infof("Total records to process: %d", totalRecords)

	batchSize := 10000 // ประมวลผลคราวละ 10,000 records
	jobQueue := "processing_jobs"

	// สร้าง batch job record
	batchJob := batch.BatchJob{
		BatchName:    "Massive Data Processing",
		Status:       "processing",
		TotalRecords: int(totalRecords),
		StartedAt:    time.Now(),
	}

	if err := jp.db.Create(&batchJob).Error; err != nil {
		return fmt.Errorf("failed to create batch job: %w", err)
	}

	// แบ่งงานเป็น chunks และส่งไป Redis queue
	jobCount := 0
	for startID := int64(1); startID <= totalRecords; startID += int64(batchSize) {
		endID := startID + int64(batchSize) - 1
		if endID > totalRecords {
			endID = totalRecords
		}

		job := ProcessingJob{
			JobID:     fmt.Sprintf("job_%d_%d", startID, endID),
			StartID:   startID,
			EndID:     endID,
			BatchSize: batchSize,
			JobType:   "data_processing",
			CreatedAt: time.Now(),
		}

		jobData, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %w", err)
		}

		if err := jp.redis.LPush(ctx, jobQueue, jobData).Err(); err != nil {
			return fmt.Errorf("failed to push job to queue: %w", err)
		}

		jobCount++
		if jobCount%100 == 0 {
			jp.log.Infof("Published %d jobs", jobCount)
		}
	}

	jp.log.Infof("Successfully published %d jobs to queue", jobCount)
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	ctx := context.Background()
	publisher := NewJobPublisher()

	if err := publisher.PublishJobs(ctx); err != nil {
		logrus.Fatal("Failed to publish jobs:", err)
	}
}
