#!/usr/bin/env python3
import csv
import random
import string
from datetime import datetime, timedelta
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import uuid
import time

class DataGenerator:
    def __init__(self):
        # เพิ่มชื่อให้เยอะขึ้น
        self.names = [
            'John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry',
            'Ivy', 'Jack', 'Kate', 'Leo', 'Mia', 'Noah', 'Olivia', 'Paul', 'Quinn', 'Ruby',
            'Sam', 'Tina', 'Uma', 'Victor', 'Wendy', 'Xavier', 'Yara', 'Zoe', 'Aaron', 'Beth'
        ]
        self.domains = ['gmail.com', 'hotmail.com', 'yahoo.com', 'company.com', 'email.com']
        self.statuses = ['pending', 'processing', 'completed', 'failed', 'cancelled']
        
    def generate_random_string(self, length=8):
        return ''.join(random.choices(string.ascii_lowercase, k=length))
    
    def generate_record(self, record_id):
        name = random.choice(self.names)
        # ใช้ record_id รับประกันความไม่ซ้ำ
        email = f"{name.lower()}.{record_id}@{random.choice(self.domains)}"
        age = random.randint(18, 80)
        salary = random.randint(30000, 150000)
        created_at = datetime.now() - timedelta(days=random.randint(0, 365))
        batch_status = random.choice(self.statuses)
        batch_id = random.randint(1, 1000)
        
        return [
            record_id,
            name,
            email,
            age,
            salary,
            created_at.strftime('%Y-%m-%d %H:%M:%S'),
            batch_status,
            batch_id
        ]

def generate_chunk(args):
    start_id, chunk_size, output_dir = args
    generator = DataGenerator()
    filename = f"{output_dir}/data_chunk_{start_id//chunk_size + 1}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header only for first chunk
        if start_id == 0:
            writer.writerow(['id', 'name', 'email', 'age', 'salary', 'created_at', 'batch_status', 'batch_id'])
        
        for i in range(start_id, start_id + chunk_size):
            record = generator.generate_record(i + 1)
            writer.writerow(record)
    
    print(f"Generated chunk {start_id//chunk_size + 1} with {chunk_size} records")
    return filename

def generate_csv_data(total_records=100_000_000, chunk_size=1_000_000, output_dir="data"):
    """Generate 100M records in parallel chunks"""
    print(f"Generating {total_records:,} records in chunks of {chunk_size:,}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate arguments for parallel processing
    chunks = []
    for start_id in range(0, total_records, chunk_size):
        actual_chunk_size = min(chunk_size, total_records - start_id)
        chunks.append((start_id, actual_chunk_size, output_dir))
    
    # Use ProcessPoolExecutor for CPU-intensive work
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        files = executor.map(generate_chunk, chunks)
    
    print(f"Generated {len(chunks)} files in {output_dir}/")
    return list(files)

def create_database_schema():
    """Create database table with batch support"""
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    
    cursor = conn.cursor()
    
    # Create main table
    cursor.execute("""
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
    """)
    
    # Create batch tracking table
    cursor.execute("""
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
    """)
    
    # Create indexes for better performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_batch_status ON users(batch_status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_batch_id ON users(batch_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON batch_jobs(status);")
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Database schema created successfully!")

def bulk_insert_to_postgres(csv_files, batch_size=10000):
    """Efficiently insert CSV data to PostgreSQL using COPY"""
    conn = psycopg2.connect(
        host="localhost",
        port="5432", 
        database="postgres",
        user="postgres",
        password="postgres"
    )
    
    cursor = conn.cursor()
    
    # Create batch job record
    cursor.execute("""
        INSERT INTO batch_jobs (batch_name, status, total_records) 
        VALUES (%s, %s, %s) RETURNING id
    """, ("Data Seed Import", "processing", len(csv_files)))
    
    batch_job_id = cursor.fetchone()[0]
    conn.commit()
    
    try:
        total_inserted = 0
        
        for csv_file in csv_files:
            print(f"Processing {csv_file}...")
            
            # Use COPY for fastest bulk insert
            with open(csv_file, 'r') as f:
                # Skip header if it exists
                next(f, None)
                cursor.copy_expert("""
                    COPY users (id, name, email, age, salary, created_at, batch_status, batch_id) 
                    FROM STDIN WITH CSV
                """, f)
            
            conn.commit()
            
            # Get count of records in this file
            with open(csv_file, 'r') as f:
                record_count = sum(1 for line in f) - 1  # -1 for header
            
            total_inserted += record_count
            
            # Update batch job progress
            cursor.execute("""
                UPDATE batch_jobs 
                SET processed_records = %s 
                WHERE id = %s
            """, (total_inserted, batch_job_id))
            conn.commit()
            
            print(f"Inserted {record_count:,} records from {csv_file}")
        
        # Mark batch as completed
        cursor.execute("""
            UPDATE batch_jobs 
            SET status = 'completed', completed_at = CURRENT_TIMESTAMP 
            WHERE id = %s
        """, (batch_job_id,))
        conn.commit()
        
        print(f"Successfully inserted {total_inserted:,} total records!")
        
    except Exception as e:
        # Mark batch as failed
        cursor.execute("""
            UPDATE batch_jobs 
            SET status = 'failed', error_message = %s 
            WHERE id = %s
        """, (str(e), batch_job_id))
        conn.commit()
        raise e
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import sys
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "generate"
    
    if mode == "generate":
        # Generate CSV files
        print("Starting CSV generation...")
        csv_files = generate_csv_data(
            total_records=100_000_000,  # 100M records
            chunk_size=1_000_000,       # 1M per file
            output_dir="data"
        )
        print("CSV generation completed!")
        
    elif mode == "schema":
        # Create database schema
        create_database_schema()
        
    elif mode == "import":
        # Import to database
        import glob
        csv_files = sorted(glob.glob("data/data_chunk_*.csv"))
        if csv_files:
            print(f"Found {len(csv_files)} CSV files to import")
            bulk_insert_to_postgres(csv_files)
        else:
            print("No CSV files found. Run 'python seed.py generate' first.")
    
    elif mode == "all":
        # Do everything
        print("Creating database schema...")
        create_database_schema()
        
        print("Generating CSV data...")
        csv_files = generate_csv_data()
        
        print("Importing to database...")
        bulk_insert_to_postgres(csv_files)
        
        print("All operations completed!")
