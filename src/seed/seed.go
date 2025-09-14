package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"k8s-batch/src/model"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
)

// DataGenerator สำหรับสร้างข้อมูลจำลอง
type DataGenerator struct {
	names    []string
	domains  []string
	statuses []string
	rand     *rand.Rand
}

// NewDataGenerator สร้าง DataGenerator ใหม่
func NewDataGenerator(seed int64) *DataGenerator {
	return &DataGenerator{
		names: []string{
			"John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
			"Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Ruby",
			"Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zoe", "Aaron", "Beth",
		},
		domains:  []string{"gmail.com", "hotmail.com", "yahoo.com", "company.com", "email.com"},
		statuses: []string{"pending", "completed", "failed", "cancelled"}, // status "processing" when batch job is running
		rand:     rand.New(rand.NewSource(seed)),
	}
}

// GenerateRandomString สร้าง random string ด้วยความยาวที่กำหนด
func (dg *DataGenerator) GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[dg.rand.Intn(len(charset))]
	}
	return string(b)
}

// GenerateRecord สร้าง User record
func (dg *DataGenerator) GenerateRecord(recordID int) model.User {
	name := dg.names[dg.rand.Intn(len(dg.names))]
	email := fmt.Sprintf("%s.%d@%s",
		strings.ToLower(name),
		recordID,
		dg.domains[dg.rand.Intn(len(dg.domains))])

	age := dg.rand.Intn(63) + 18           // 18-80
	salary := dg.rand.Intn(120001) + 30000 // 30,000-150,000
	createdAt := time.Now().Add(-time.Duration(dg.rand.Intn(365)) * 24 * time.Hour)
	batchStatus := dg.statuses[dg.rand.Intn(len(dg.statuses))]
	batchID := dg.rand.Intn(1000) + 1

	return model.User{
		ID:          recordID,
		Name:        name,
		Email:       email,
		Age:         age,
		Salary:      salary,
		CreatedAt:   createdAt,
		BatchStatus: batchStatus,
		BatchID:     batchID,
		UpdatedAt:   time.Now(),
	}
}

// ChunkConfig การกำหนดค่าสำหรับ chunk
type ChunkConfig struct {
	StartID   int
	ChunkSize int
	OutputDir string
	ChunkNum  int
}

// generateChunk สร้าง CSV chunk
func generateChunk(config ChunkConfig) error {
	generator := NewDataGenerator(int64(config.StartID))
	filename := filepath.Join(config.OutputDir, fmt.Sprintf("data_chunk_%d.csv", config.ChunkNum))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// เขียน header เฉพาะ chunk แรก
	if config.ChunkNum == 1 {
		header := []string{"id", "name", "email", "age", "salary", "created_at", "batch_status", "batch_id"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// เขียนข้อมูล
	for i := config.StartID; i < config.StartID+config.ChunkSize; i++ {
		record := generator.GenerateRecord(i)
		row := []string{
			strconv.Itoa(record.ID),
			record.Name,
			record.Email,
			strconv.Itoa(record.Age),
			strconv.Itoa(record.Salary),
			record.CreatedAt.Format("2006-01-02 15:04:05"),
			record.BatchStatus,
			strconv.Itoa(record.BatchID),
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	fmt.Printf("Generated chunk %d with %d records\n", config.ChunkNum, config.ChunkSize)
	return nil
}

// GenerateCSVData สร้างข้อมูล CSV แบบ parallel
func GenerateCSVData(totalRecords, chunkSize int, outputDir string) ([]string, error) {
	fmt.Printf("Generating %s records in chunks of %s\n",
		formatNumber(totalRecords), formatNumber(chunkSize))

	// สร้าง output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// คำนวณจำนวน chunks
	numChunks := (totalRecords + chunkSize - 1) / chunkSize

	// สร้าง chunk configs
	configs := make([]ChunkConfig, 0, numChunks)
	for i := 0; i < numChunks; i++ {
		startID := i*chunkSize + 1
		actualChunkSize := chunkSize
		if startID+chunkSize-1 > totalRecords {
			actualChunkSize = totalRecords - startID + 1
		}

		configs = append(configs, ChunkConfig{
			StartID:   startID,
			ChunkSize: actualChunkSize,
			OutputDir: outputDir,
			ChunkNum:  i + 1,
		})
	}

	// ประมวลผลแบบ parallel
	numWorkers := runtime.NumCPU()
	jobs := make(chan ChunkConfig, len(configs))
	results := make(chan error, len(configs))

	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for config := range jobs {
				results <- generateChunk(config)
			}
		}()
	}

	// Send jobs
	go func() {
		for _, config := range configs {
			jobs <- config
		}
		close(jobs)
	}()

	// Wait for completion
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var files []string
	for err := range results {
		if err != nil {
			return nil, fmt.Errorf("chunk generation failed: %w", err)
		}
	}

	// สร้างรายการไฟล์
	for i := 1; i <= numChunks; i++ {
		filename := filepath.Join(outputDir, fmt.Sprintf("data_chunk_%d.csv", i))
		files = append(files, filename)
	}

	fmt.Printf("Generated %d files in %s/\n", len(files), outputDir)
	return files, nil
}

// CreateDatabaseSchema สร้าง database schema
func CreateDatabaseSchema() error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_PORT", "5432"),
		getEnv("DB_USER", "postgres"),
		getEnv("DB_PASSWORD", "postgres"),
		getEnv("DB_NAME", "postgres"))

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Create main table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id BIGSERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(255) UNIQUE NOT NULL,
			age INTEGER CHECK (age >= 0 AND age <= 150),
			salary INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			batch_status VARCHAR(20) DEFAULT 'pending',
			batch_id INTEGER,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	// Create batch tracking table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS batch_jobs (
			id SERIAL PRIMARY KEY,
			batch_name VARCHAR(255) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			total_records INTEGER DEFAULT 0,
			processed_records INTEGER DEFAULT 0,
			failed_records INTEGER DEFAULT 0,
			started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP,
			error_message TEXT
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create batch_jobs table: %w", err)
	}

	// Create indexes
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_users_batch_status ON users(batch_status);",
		"CREATE INDEX IF NOT EXISTS idx_users_batch_id ON users(batch_id);",
		"CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);",
		"CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON batch_jobs(status);",
	}

	for _, idx := range indexes {
		if _, err := db.Exec(idx); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	fmt.Println("Database schema created successfully!")
	return nil
}

// ImportResult เก็บผลลัพธ์ของการ import แต่ละไฟล์
type ImportResult struct {
	Filename    string
	RecordCount int
	Error       error
}

// importSingleFile import ไฟล์ CSV เดียวไป PostgreSQL
func importSingleFile(csvFile string, connStr string) ImportResult {
	result := ImportResult{Filename: csvFile}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		result.Error = fmt.Errorf("failed to connect to database: %w", err)
		return result
	}
	defer db.Close()

	// เปิดไฟล์ CSV
	file, err := os.Open(csvFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to open CSV file: %w", err)
		return result
	}
	defer file.Close()

	// ใช้ COPY สำหรับ bulk insert ที่เร็วที่สุด
	txn, err := db.Begin()
	if err != nil {
		result.Error = fmt.Errorf("failed to begin transaction: %w", err)
		return result
	}
	defer txn.Rollback() // Safe to call even after commit

	stmt, err := txn.Prepare(pq.CopyIn("users",
		"id", "name", "email", "age", "salary", "created_at", "batch_status", "batch_id"))
	if err != nil {
		result.Error = fmt.Errorf("failed to prepare COPY statement: %w", err)
		return result
	}
	defer stmt.Close()

	reader := csv.NewReader(file)

	// Skip header if exists
	firstRow, err := reader.Read()
	if err != nil {
		result.Error = fmt.Errorf("failed to read first row: %w", err)
		return result
	}

	recordCount := 0

	// Check if first row is header
	if firstRow[0] != "id" {
		// First row is data, process it
		if err := processCopyRow(stmt, firstRow); err != nil {
			result.Error = fmt.Errorf("failed to process row: %w", err)
			return result
		}
		recordCount++
	}

	// Process remaining rows
	for {
		row, err := reader.Read()
		if err != nil {
			break // EOF or error
		}

		if err := processCopyRow(stmt, row); err != nil {
			result.Error = fmt.Errorf("failed to process row: %w", err)
			return result
		}
		recordCount++
	}

	// Execute the COPY
	if _, err := stmt.Exec(); err != nil {
		result.Error = fmt.Errorf("failed to execute COPY: %w", err)
		return result
	}

	if err := stmt.Close(); err != nil {
		result.Error = fmt.Errorf("failed to close COPY statement: %w", err)
		return result
	}

	if err := txn.Commit(); err != nil {
		result.Error = fmt.Errorf("failed to commit transaction: %w", err)
		return result
	}

	result.RecordCount = recordCount
	fmt.Printf("Inserted %s records from %s\n", formatNumber(recordCount), csvFile)
	return result
}

// BulkInsertToPostgres นำเข้าข้อมูล CSV ไป PostgreSQL แบบ parallel
func BulkInsertToPostgres(csvFiles []string) error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		getEnv("DB_HOST", "localhost"),
		getEnv("DB_PORT", "5432"),
		getEnv("DB_USER", "postgres"),
		getEnv("DB_PASSWORD", "postgres"),
		getEnv("DB_NAME", "postgres"))

	// Create batch job record
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	var batchJobID int
	err = db.QueryRow(`
		INSERT INTO batch_jobs (batch_name, status, total_records) 
		VALUES ($1, $2, $3) RETURNING id
	`, "Data Seed Import", "processing", len(csvFiles)).Scan(&batchJobID)
	if err != nil {
		return fmt.Errorf("failed to create batch job: %w", err)
	}

	fmt.Printf("Starting parallel import of %d files...\n", len(csvFiles))

	// กำหนดจำนวน workers (สามารถปรับได้ตาม DB connection limit)
	numWorkers := runtime.NumCPU()
	if numWorkers > 10 { // จำกัดไม่ให้เกิน 10 connections พร้อมกัน
		numWorkers = 10
	}

	// Create channels
	jobs := make(chan string, len(csvFiles))
	results := make(chan ImportResult, len(csvFiles))

	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			fmt.Printf("Worker %d started\n", workerID)
			for csvFile := range jobs {
				result := importSingleFile(csvFile, connStr)
				results <- result
			}
			fmt.Printf("Worker %d finished\n", workerID)
		}(w)
	}

	// Send jobs
	go func() {
		for _, csvFile := range csvFiles {
			jobs <- csvFile
		}
		close(jobs)
	}()

	// Wait for completion
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	totalInserted := 0
	processedFiles := 0
	var importErrors []error

	for result := range results {
		processedFiles++
		if result.Error != nil {
			importErrors = append(importErrors, fmt.Errorf("file %s: %w", result.Filename, result.Error))
			continue
		}

		totalInserted += result.RecordCount

		// Update batch job progress
		if processedFiles%10 == 0 || processedFiles == len(csvFiles) { // Update every 10 files or at the end
			_, err = db.Exec(`
				UPDATE batch_jobs 
				SET processed_records = $1 
				WHERE id = $2
			`, totalInserted, batchJobID)
			if err != nil {
				fmt.Printf("Warning: failed to update batch progress: %v\n", err)
			}
			fmt.Printf("Progress: %d/%d files processed, %s records inserted\n",
				processedFiles, len(csvFiles), formatNumber(totalInserted))
		}
	}

	// Check for errors
	if len(importErrors) > 0 {
		// Mark batch as failed
		_, err = db.Exec(`
			UPDATE batch_jobs 
			SET status = 'failed', completed_at = CURRENT_TIMESTAMP, failed_records = $1,
			    error_message = $2
			WHERE id = $3
		`, len(importErrors), fmt.Sprintf("Failed files: %d", len(importErrors)), batchJobID)
		if err != nil {
			fmt.Printf("Warning: failed to update batch status: %v\n", err)
		}

		// Return first error for simplicity
		return fmt.Errorf("import failed for %d files, first error: %w", len(importErrors), importErrors[0])
	}

	// Mark batch as completed
	_, err = db.Exec(`
		UPDATE batch_jobs 
		SET status = 'completed', completed_at = CURRENT_TIMESTAMP 
		WHERE id = $1
	`, batchJobID)
	if err != nil {
		return fmt.Errorf("failed to mark batch as completed: %w", err)
	}

	fmt.Printf("Successfully imported %s total records from %d files!\n",
		formatNumber(totalInserted), len(csvFiles))
	return nil
}

// processCopyRow ประมวลผล row สำหรับ COPY command
func processCopyRow(stmt *sql.Stmt, row []string) error {
	if len(row) != 8 {
		return fmt.Errorf("expected 8 columns, got %d", len(row))
	}

	id, err := strconv.Atoi(row[0])
	if err != nil {
		return fmt.Errorf("invalid id: %w", err)
	}

	age, err := strconv.Atoi(row[3])
	if err != nil {
		return fmt.Errorf("invalid age: %w", err)
	}

	salary, err := strconv.Atoi(row[4])
	if err != nil {
		return fmt.Errorf("invalid salary: %w", err)
	}

	createdAt, err := time.Parse("2006-01-02 15:04:05", row[5])
	if err != nil {
		return fmt.Errorf("invalid created_at: %w", err)
	}

	batchID, err := strconv.Atoi(row[7])
	if err != nil {
		return fmt.Errorf("invalid batch_id: %w", err)
	}

	_, err = stmt.Exec(id, row[1], row[2], age, salary, createdAt, row[6], batchID)
	return err
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func formatNumber(n int) string {
	str := strconv.Itoa(n)
	if len(str) <= 3 {
		return str
	}

	result := ""
	for i, char := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(char)
	}
	return result
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run seed.go [generate|schema|import|all]")
		os.Exit(1)
	}

	mode := os.Args[1]

	switch mode {
	case "generate":
		// Generate CSV files
		fmt.Println("Starting CSV generation...")
		_, err := GenerateCSVData(
			50_000_000, // 100M records
			1_000_000,  // 1M per file
			"data",     // output directory
		)
		if err != nil {
			log.Fatal("CSV generation failed:", err)
		}
		fmt.Println("CSV generation completed!")

	case "schema":
		// Create database schema
		if err := CreateDatabaseSchema(); err != nil {
			log.Fatal("Schema creation failed:", err)
		}

	case "import":
		// Import to database
		files, err := filepath.Glob("data/data_chunk_*.csv")
		if err != nil {
			log.Fatal("Failed to find CSV files:", err)
		}

		if len(files) == 0 {
			fmt.Println("No CSV files found. Run 'go run seed.go generate' first.")
			os.Exit(1)
		}

		fmt.Printf("Found %d CSV files to import\n", len(files))
		if err := BulkInsertToPostgres(files); err != nil {
			log.Fatal("Import failed:", err)
		}

	case "all":
		// Do everything
		fmt.Println("Creating database schema...")
		if err := CreateDatabaseSchema(); err != nil {
			log.Fatal("Schema creation failed:", err)
		}

		fmt.Println("Generating CSV data...")
		files, err := GenerateCSVData(100_000_000, 1_000_000, "data")
		if err != nil {
			log.Fatal("CSV generation failed:", err)
		}

		fmt.Println("Importing to database...")
		if err := BulkInsertToPostgres(files); err != nil {
			log.Fatal("Import failed:", err)
		}

		fmt.Println("All operations completed!")

	default:
		fmt.Println("Invalid mode. Use: generate, schema, import, or all")
		os.Exit(1)
	}
}
