package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
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

// =========================
// Config
// =========================

type Config struct {
	PGDsn         string
	ClaimSize     int
	Workers       int
	ClaimTimeout  time.Duration
	ShutdownGrace time.Duration
	PodName       string
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustAtoi(name, v string) int {
	i, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("env %s must be int: %v", name, err)
	}
	return i
}

func mustParseDur(name, v string) time.Duration {
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("env %s must be duration: %v", name, err)
	}
	return d
}

func loadConfig() Config {
	return Config{
		PGDsn:         getEnv("PG_DSN", "postgres://user:pass@postgres:5432/app?sslmode=disable"),
		ClaimSize:     mustAtoi("CLAIM_SIZE", getEnv("CLAIM_SIZE", "20000")),
		Workers:       mustAtoi("WORKERS", getEnv("WORKERS", "8")),
		ClaimTimeout:  mustParseDur("CLAIM_TIMEOUT_S", getEnv("CLAIM_TIMEOUT_S", "30s")),
		ShutdownGrace: mustParseDur("SHUTDOWN_GRACE_S", getEnv("SHUTDOWN_GRACE_S", "30s")),
		PodName:       getEnv("POD_NAME", fmt.Sprintf("worker-%d", time.Now().Unix())),
	}
}

// =========================
// DB
// =========================

type DB struct {
	*gorm.DB
}

func newDB(ctx context.Context, dsn string) (*DB, error) {
	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
		},
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(16)
	sqlDB.SetMaxIdleConns(4)
	sqlDB.SetConnMaxLifetime(time.Hour)

	return &DB{db}, nil
}

// =========================
// Batch Job Management
// =========================

func (db *DB) createBatchJob(ctx context.Context, batchName string) (*model.BatchJob, error) {
	batch := &model.BatchJob{
		BatchName:        batchName,
		Status:           "running",
		TotalRecords:     0,
		ProcessedRecords: 0,
		FailedRecords:    0,
		StartedAt:        time.Now(),
	}

	err := db.WithContext(ctx).Create(batch).Error
	if err != nil {
		return nil, err
	}

	slog.Info("created batch job", "batch_id", batch.ID, "batch_name", batch.BatchName)
	return batch, nil
}

func (db *DB) updateBatchJobProgress(ctx context.Context, batchID int, processed, failed int) error {
	return db.WithContext(ctx).Model(&model.BatchJob{}).
		Where("id = ?", batchID).
		Updates(map[string]interface{}{
			"processed_records": gorm.Expr("processed_records + ?", processed),
			"failed_records":    gorm.Expr("failed_records + ?", failed),
		}).Error
}

func (db *DB) completeBatchJob(ctx context.Context, batchID int, status string, errorMsg *string) error {
	updates := map[string]interface{}{
		"status":       status,
		"completed_at": time.Now(),
	}

	if errorMsg != nil {
		updates["error_message"] = *errorMsg
	}

	return db.WithContext(ctx).Model(&model.BatchJob{}).
		Where("id = ?", batchID).
		Updates(updates).Error
}

// =========================
// User Processing
// =========================

func (db *DB) claimIDs(ctx context.Context, batchID int, claimSize int) ([]int64, error) {
	var users []model.User

	// Use raw SQL for atomic claim operation with SKIP LOCKED
	err := db.WithContext(ctx).Raw(`
		WITH picked AS (
			SELECT id
			FROM users
			WHERE batch_status IN ('pending', 'failed')
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT ?
		)
		UPDATE users u
		SET batch_status = 'processing',
			batch_id = ?,
			updated_at = NOW()
		FROM picked
		WHERE u.id = picked.id
		RETURNING u.id
	`, claimSize, batchID).Scan(&users).Error

	if err != nil {
		return nil, err
	}

	ids := make([]int64, len(users))
	for i, user := range users {
		ids[i] = int64(user.ID)
	}
	return ids, nil
}

func (db *DB) markCompleted(ctx context.Context, batchID int, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	result := db.WithContext(ctx).Model(&model.User{}).
		Where("id IN ? AND batch_id = ?", ids, batchID).
		Updates(map[string]interface{}{
			"batch_status": "completed",
			"updated_at":   time.Now(),
		})

	return result.Error
}

type failItem struct {
	ID    int64
	Error string
}

func (db *DB) markFailed(ctx context.Context, batchID int, fails []failItem) error {
	if len(fails) == 0 {
		return nil
	}

	ids := make([]int64, len(fails))
	for i, f := range fails {
		ids[i] = f.ID
	}

	result := db.WithContext(ctx).Model(&model.User{}).
		Where("id IN ? AND batch_id = ?", ids, batchID).
		Updates(map[string]interface{}{
			"batch_status": "failed",
			"updated_at":   time.Now(),
		})

	return result.Error
}

// =========================
// Processing
// =========================

type result struct {
	id  int64
	err error
}

func processOne(ctx context.Context) error {
	// TODO: ใส่ลอจิกจริงของงานที่ต้องทำกับ record id นี้
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Millisecond):
	}
	// return errors.New("mock failure") // เปิดไว้ทดสอบเส้นทาง failed
	return nil
}

func runBatch(ctx context.Context, db *DB, cfg Config, batchJob *model.BatchJob) error {
	claimCtx, cancel := context.WithTimeout(ctx, cfg.ClaimTimeout)
	defer cancel()

	ids, err := db.claimIDs(claimCtx, batchJob.ID, cfg.ClaimSize)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if len(ids) == 0 {
		slog.Info("no more pending rows; completing batch", "batch_id", batchJob.ID)
		return db.completeBatchJob(ctx, batchJob.ID, "completed", nil)
	}
	slog.Info("claimed", "batch_id", batchJob.ID, "count", len(ids))

	// Worker pool
	tasks := make(chan int64, len(ids))
	results := make(chan result, len(ids))

	// spawn workers
	workers := cfg.Workers
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		go func() {
			for id := range tasks {
				var lastErr error
				for attempt := 1; attempt <= 3; attempt++ {
					perItemCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					lastErr = processOne(perItemCtx)
					cancel()
					if lastErr == nil {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				results <- result{id: id, err: lastErr}
			}
		}()
	}

	for _, id := range ids {
		tasks <- id
	}
	close(tasks)

	var completed []int64
	var fails []failItem

	for i := 0; i < len(ids); i++ {
		r := <-results
		if r.err == nil {
			completed = append(completed, r.id)
		} else {
			fails = append(fails, failItem{ID: r.id, Error: r.err.Error()})
		}
	}

	// อัปเดตสถานะกลับ DB
	if err := db.markCompleted(ctx, batchJob.ID, completed); err != nil {
		return fmt.Errorf("markCompleted: %w", err)
	}
	if err := db.markFailed(ctx, batchJob.ID, fails); err != nil {
		return fmt.Errorf("markFailed: %w", err)
	}

	// อัปเดต batch job progress
	if err := db.updateBatchJobProgress(ctx, batchJob.ID, len(completed), len(fails)); err != nil {
		return fmt.Errorf("updateBatchJobProgress: %w", err)
	}

	slog.Info("batch iteration done",
		"batch_id", batchJob.ID,
		"claimed", len(ids),
		"completed", len(completed),
		"failed", len(fails),
	)
	return nil
}

// =========================
// Main
// =========================

func main() {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(handler))

	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := newDB(ctx, cfg.PGDsn)
	if err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer func() {
		sqlDB, _ := db.DB.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}()

	// สร้าง batch job สำหรับ worker นี้
	batchName := fmt.Sprintf("%s-%s", cfg.PodName, time.Now().Format("20060102-150405"))
	batchJob, err := db.createBatchJob(ctx, batchName)
	if err != nil {
		log.Fatalf("create batch job: %v", err)
	}

	slog.Info("worker started", "pod_name", cfg.PodName, "batch_id", batchJob.ID, "batch_name", batchName)

	var lastErr error
	defer func() {
		// Complete batch job เมื่อจบการทำงาน
		status := "completed"
		var errorMsg *string
		if lastErr != nil {
			status = "failed"
			errStr := lastErr.Error()
			errorMsg = &errStr
		}

		if err := db.completeBatchJob(context.Background(), batchJob.ID, status, errorMsg); err != nil {
			slog.Error("failed to complete batch job", "err", err)
		}
		slog.Info("worker finished", "batch_id", batchJob.ID, "status", status)
	}()

	for {
		select {
		case <-ctx.Done():
			slog.Warn("shutting down (signal)")
			lastErr = errors.New("shutdown by signal")
			grace(ctx, cfg.ShutdownGrace)
			return
		default:
			if err := runBatch(ctx, db, cfg, batchJob); err != nil {
				if errors.Is(err, context.Canceled) {
					slog.Warn("context canceled")
					lastErr = err
					grace(ctx, cfg.ShutdownGrace)
					return
				}
				slog.Error("runBatch error; backing off", "err", err, "batch_id", batchJob.ID)
				lastErr = err
				time.Sleep(2 * time.Second)
			} else {
				// ถ้า runBatch สำเร็จและไม่มี pending records แล้ว ให้จบการทำงาน
				return
			}
		}
	}
}

func grace(ctx context.Context, d time.Duration) {
	slog.Info("grace period", "seconds", d.Seconds())
	timer := time.NewTimer(d)
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
