package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"k8s-batch/src/model"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
)

type Worker struct {
	db              *gorm.DB
	claimLimit      int
	sleepMS         int
	leaseSeconds    int
	batchID         sql.NullInt64
	workerID        string
	processingLimit int // Maximum records to process before stopping
}

func NewWorker(db *gorm.DB) *Worker {
	claimLimit, _ := strconv.Atoi(getEnv("CLAIM_LIMIT", "5000"))
	sleepMS, _ := strconv.Atoi(getEnv("EMPTY_SLEEP_MS", "1500"))
	leaseSeconds, _ := strconv.Atoi(getEnv("LEASE_SECONDS", "600"))
	processingLimit, _ := strconv.Atoi(getEnv("PROCESSING_LIMIT", "0")) // 0 = unlimited

	var batchID sql.NullInt64
	if batchIDStr := getEnv("BATCH_ID", ""); batchIDStr != "" {
		if v, err := strconv.ParseInt(batchIDStr, 10, 64); err == nil {
			batchID = sql.NullInt64{Int64: v, Valid: true}
		}
	}

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	return &Worker{
		db:              db,
		claimLimit:      claimLimit,
		sleepMS:         sleepMS,
		leaseSeconds:    leaseSeconds,
		batchID:         batchID,
		workerID:        workerID,
		processingLimit: processingLimit,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	log.Printf("Worker %s starting with claim_limit=%d, lease_seconds=%d",
		w.workerID, w.claimLimit, w.leaseSeconds)

	totalProcessed := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s shutting down gracefully. Processed %d records total",
				w.workerID, totalProcessed)
			return nil
		default:
		}

		// Check processing limit
		if w.processingLimit > 0 && totalProcessed >= w.processingLimit {
			log.Printf("Worker %s reached processing limit of %d records",
				w.workerID, w.processingLimit)
			return nil
		}

		// Claim work
		ids, err := w.claimWork(ctx)
		if err != nil {
			log.Printf("Worker %s failed to claim work: %v", w.workerID, err)
			time.Sleep(time.Duration(w.sleepMS) * time.Millisecond)
			continue
		}

		if len(ids) == 0 {
			log.Printf("Worker %s: no work available, sleeping...", w.workerID)
			time.Sleep(time.Duration(w.sleepMS) * time.Millisecond)
			continue
		}

		log.Printf("Worker %s claimed %d records for processing", w.workerID, len(ids))

		// Process the work
		processed, failed := w.processWork(ctx, ids)
		totalProcessed += processed + failed

		log.Printf("Worker %s processed %d records (%d success, %d failed). Total: %d",
			w.workerID, len(ids), processed, failed, totalProcessed)

		// Cleanup stale processing records periodically
		if totalProcessed%10000 == 0 {
			if err := w.cleanupStaleWork(ctx); err != nil {
				log.Printf("Worker %s failed to cleanup stale work: %v", w.workerID, err)
			}
		}
	}
}

func (w *Worker) claimWork(ctx context.Context) ([]int, error) {
	tx := w.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer tx.Rollback()

	// Build query based on whether we have a specific batch_id
	var query string
	var args []interface{}

	if w.batchID.Valid {
		query = `
		WITH cte AS (
			SELECT id
			FROM users
			WHERE batch_status IN ('pending', 'failed') 
			AND (batch_id = ? OR batch_id IS NULL)
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT ?
		)
		UPDATE users u
		SET batch_status = 'processing',
			updated_at = NOW(),
			batch_id = ?
		FROM cte
		WHERE u.id = cte.id
		RETURNING u.id;`
		args = []interface{}{w.batchID.Int64, w.claimLimit, w.batchID.Int64}
	} else {
		query = `
		WITH cte AS (
			SELECT id
			FROM users
			WHERE batch_status IN ('pending', 'failed')
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT ?
		)
		UPDATE users u
		SET batch_status = 'processing',
			updated_at = NOW()
		FROM cte
		WHERE u.id = cte.id
		RETURNING u.id;`
		args = []interface{}{w.claimLimit}
	}

	rows, err := tx.Raw(query, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	return ids, nil
}

func (w *Worker) processWork(ctx context.Context, ids []int) (processed, failed int) {
	if len(ids) == 0 {
		return 0, 0
	}

	// Simulate processing work
	// In real implementation, you would:
	// 1. Fetch the records
	// 2. Apply business logic
	// 3. Call external APIs
	// 4. Transform data
	// 5. Update status based on success/failure

	batchSize := 1000
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		batch := ids[i:end]

		// Simulate work with some processing time
		processingTime := time.Duration(len(batch)) * time.Millisecond * 5 // 5ms per record
		time.Sleep(processingTime)

		// For this example, mark all as completed
		// In real world, you might have some failures
		err := w.db.WithContext(ctx).
			Model(&model.User{}).
			Where("id IN ?", batch).
			Updates(map[string]interface{}{
				"batch_status": StatusCompleted,
				"updated_at":   time.Now(),
			}).Error

		if err != nil {
			log.Printf("Worker %s failed to update batch %v: %v", w.workerID, batch, err)
			// Mark as failed
			w.db.WithContext(ctx).
				Model(&model.User{}).
				Where("id IN ?", batch).
				Updates(map[string]interface{}{
					"batch_status": StatusFailed,
					"updated_at":   time.Now(),
				})
			failed += len(batch)
		} else {
			processed += len(batch)
		}
	}

	return processed, failed
}

func (w *Worker) cleanupStaleWork(ctx context.Context) error {
	leaseTimeout := time.Now().Add(-time.Duration(w.leaseSeconds) * time.Second)

	result := w.db.WithContext(ctx).
		Model(&model.User{}).
		Where("batch_status = ? AND updated_at < ?", StatusProcessing, leaseTimeout).
		Updates(map[string]interface{}{
			"batch_status": StatusPending,
			"updated_at":   time.Now(),
		})

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected > 0 {
		log.Printf("Worker %s requeued %d stale processing records",
			w.workerID, result.RowsAffected)
	}

	return nil
}

func main() {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN environment variable is required")
	}

	// Configure GORM with custom logger
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn), // Only log warnings and errors
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	worker := NewWorker(db)

	// Setup graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	if err := worker.Run(ctx); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}

	log.Println("Worker stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
