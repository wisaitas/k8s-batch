package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"k8s-batch/src/model"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusCancelled  = "cancelled"
)

type BatchScheduler struct {
	db                     *gorm.DB
	recordsPerPod          int
	maxPods                int
	minPods                int
	processingTimeoutHours int
}

func NewBatchScheduler(db *gorm.DB) *BatchScheduler {
	recordsPerPod, _ := strconv.Atoi(getEnv("RECORDS_PER_POD", "10000000")) // 10M records per pod
	maxPods, _ := strconv.Atoi(getEnv("MAX_PODS", "20"))
	minPods, _ := strconv.Atoi(getEnv("MIN_PODS", "1"))
	timeoutHours, _ := strconv.Atoi(getEnv("PROCESSING_TIMEOUT_HOURS", "6"))

	return &BatchScheduler{
		db:                     db,
		recordsPerPod:          recordsPerPod,
		maxPods:                maxPods,
		minPods:                minPods,
		processingTimeoutHours: timeoutHours,
	}
}

func (bs *BatchScheduler) CreateBatchJob(ctx context.Context, batchName string) (*model.BatchJob, error) {
	// Count pending and failed records
	var totalRecords int64
	err := bs.db.WithContext(ctx).Model(&model.User{}).
		Where("batch_status IN ?", []string{StatusPending, StatusFailed}).
		Count(&totalRecords).Error
	if err != nil {
		return nil, fmt.Errorf("failed to count pending records: %w", err)
	}

	if totalRecords == 0 {
		log.Println("No records to process")
		return nil, nil
	}

	// Calculate optimal number of pods
	optimalPods := int(math.Ceil(float64(totalRecords) / float64(bs.recordsPerPod)))
	if optimalPods > bs.maxPods {
		optimalPods = bs.maxPods
	}
	if optimalPods < bs.minPods {
		optimalPods = bs.minPods
	}

	// Create batch job record
	batchJob := &model.BatchJob{
		BatchName:        batchName,
		Status:           StatusPending,
		TotalRecords:     int(totalRecords),
		ProcessedRecords: 0,
		FailedRecords:    0,
		StartedAt:        time.Now(),
	}

	if err := bs.db.WithContext(ctx).Create(batchJob).Error; err != nil {
		return nil, fmt.Errorf("failed to create batch job: %w", err)
	}

	log.Printf("Created batch job %d: %s with %d records (optimal pods: %d)",
		batchJob.ID, batchName, totalRecords, optimalPods)

	return batchJob, nil
}

func (bs *BatchScheduler) GetOptimalPodCount(ctx context.Context) (int, error) {
	var totalRecords int64
	err := bs.db.WithContext(ctx).Model(&model.User{}).
		Where("batch_status IN ?", []string{StatusPending, StatusFailed}).
		Count(&totalRecords).Error
	if err != nil {
		return 0, fmt.Errorf("failed to count pending records: %w", err)
	}

	if totalRecords == 0 {
		return 0, nil
	}

	optimalPods := int(math.Ceil(float64(totalRecords) / float64(bs.recordsPerPod)))
	if optimalPods > bs.maxPods {
		optimalPods = bs.maxPods
	}
	if optimalPods < bs.minPods {
		optimalPods = bs.minPods
	}

	return optimalPods, nil
}

func (bs *BatchScheduler) UpdateBatchProgress(ctx context.Context, batchID int) error {
	var stats struct {
		Processed int64
		Failed    int64
		Total     int64
	}

	// Count current status
	err := bs.db.WithContext(ctx).Raw(`
		SELECT 
			COUNT(CASE WHEN batch_status = 'completed' THEN 1 END) as processed,
			COUNT(CASE WHEN batch_status = 'failed' THEN 1 END) as failed,
			COUNT(*) as total
		FROM users 
		WHERE batch_id = ?`, batchID).Scan(&stats).Error

	if err != nil {
		return fmt.Errorf("failed to get batch stats: %w", err)
	}

	// Update batch job
	updates := map[string]interface{}{
		"processed_records": stats.Processed,
		"failed_records":    stats.Failed,
	}

	// Check if batch is completed
	if stats.Processed+stats.Failed >= stats.Total {
		updates["status"] = StatusCompleted
		updates["completed_at"] = time.Now()
	}

	return bs.db.WithContext(ctx).Model(&model.BatchJob{}).
		Where("id = ?", batchID).
		Updates(updates).Error
}

func (bs *BatchScheduler) CleanupStaleBatches(ctx context.Context) error {
	// Mark processing records as failed if they've been processing too long
	timeout := time.Now().Add(-time.Duration(bs.processingTimeoutHours) * time.Hour)

	result := bs.db.WithContext(ctx).
		Model(&model.User{}).
		Where("batch_status = ? AND updated_at < ?", StatusProcessing, timeout).
		Updates(map[string]interface{}{
			"batch_status": StatusFailed,
			"updated_at":   time.Now(),
		})

	if result.Error != nil {
		return fmt.Errorf("failed to cleanup stale batches: %w", result.Error)
	}

	if result.RowsAffected > 0 {
		log.Printf("Marked %d stale processing records as failed", result.RowsAffected)
	}

	return nil
}

func main() {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN environment variable is required")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	scheduler := NewBatchScheduler(db)
	ctx := context.Background()

	// Get command
	if len(os.Args) < 2 {
		log.Fatal("Usage: scheduler <create-batch|cleanup|pod-count>")
	}

	command := os.Args[1]

	switch command {
	case "create-batch":
		batchName := fmt.Sprintf("batch-%s", time.Now().Format("20060102-150405"))
		batchJob, err := scheduler.CreateBatchJob(ctx, batchName)
		if err != nil {
			log.Fatalf("Failed to create batch job: %v", err)
		}
		if batchJob != nil {
			fmt.Printf("Created batch job: %d\n", batchJob.ID)
		} else {
			fmt.Println("No records to process")
		}

	case "cleanup":
		if err := scheduler.CleanupStaleBatches(ctx); err != nil {
			log.Fatalf("Failed to cleanup stale batches: %v", err)
		}
		fmt.Println("Cleanup completed")

	case "pod-count":
		count, err := scheduler.GetOptimalPodCount(ctx)
		if err != nil {
			log.Fatalf("Failed to get optimal pod count: %v", err)
		}
		fmt.Printf("%d\n", count)

	default:
		log.Fatal("Unknown command. Use: create-batch, cleanup, or pod-count")
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
