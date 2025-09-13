package batch

import "time"

type User struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Email       string    `json:"email"`
	Age         int       `json:"age"`
	Salary      int       `json:"salary"`
	CreatedAt   time.Time `json:"created_at"`
	BatchStatus string    `json:"batch_status"`
	BatchID     int       `json:"batch_id"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type BatchJob struct {
	ID               int       `json:"id"`
	BatchName        string    `json:"batch_name"`
	Status           string    `json:"status"`
	TotalRecords     int       `json:"total_records"`
	ProcessedRecords int       `json:"processed_records"`
	FailedRecords    int       `json:"failed_records"`
	StartedAt        time.Time `json:"started_at"`
	CompletedAt      time.Time `json:"completed_at"`
	ErrorMessage     string    `json:"error_message"`
}
