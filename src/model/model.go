package model

import (
	"database/sql"
	"time"
)

type User struct {
	ID          int       `json:"id" gorm:"column:id;primaryKey"`
	Name        string    `json:"name" gorm:"column:name"`
	Email       string    `json:"email" gorm:"column:email;uniqueIndex"`
	Age         int       `json:"age" gorm:"column:age"`
	Salary      int       `json:"salary" gorm:"column:salary"`
	CreatedAt   time.Time `json:"created_at" gorm:"column:created_at"`
	BatchStatus string    `json:"batch_status" gorm:"column:batch_status"`
	BatchID     int       `json:"batch_id" gorm:"column:batch_id"`
	UpdatedAt   time.Time `json:"updated_at" gorm:"column:updated_at"`
}

type BatchJob struct {
	ID               int            `json:"id" gorm:"column:id;primaryKey"`
	BatchName        string         `json:"batch_name" gorm:"column:batch_name"`
	Status           string         `json:"status" gorm:"column:status"`
	TotalRecords     int            `json:"total_records" gorm:"column:total_records"`
	ProcessedRecords int            `json:"processed_records" gorm:"column:processed_records"`
	FailedRecords    int            `json:"failed_records" gorm:"column:failed_records"`
	StartedAt        time.Time      `json:"started_at" gorm:"column:started_at"`
	CompletedAt      sql.NullTime   `json:"completed_at" gorm:"column:completed_at"`
	ErrorMessage     sql.NullString `json:"error_message" gorm:"column:error_message"`
}

func (User) TableName() string {
	return "users"
}

func (BatchJob) TableName() string {
	return "batch_jobs"
}
