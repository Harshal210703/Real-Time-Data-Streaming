# Real-Time Data Streaming

This project demonstrates two approaches to building a real-time data streaming pipeline for system performance metrics:

1. **Traditional Kafka-based Pipeline**: Uses Kafka for message streaming with direct producer and consumer scripts
2. **Temporal Workflow-based Pipeline**: Uses Temporal for orchestration with Kafka for the underlying message streaming

Both solutions stream real-time system metrics (CPU, memory, disk, network usage) to Kafka and store them in PostgreSQL for analysis.

## Architecture Overview

### Components in Common
- **Kafka**: Message broker (2.13-4.0.0)
- **PostgreSQL**: Data storage
- **Docker**: Container management 
- **Python 3.13.1**: Application code

### Traditional Kafka Pipeline
![Traditional Architecture]
```
[System Metrics] → [Producer Script] → [Kafka] → [Consumer Script] → [PostgreSQL]
```

### Temporal Workflow Pipeline
![Temporal Architecture]
```
[Temporal Workflow]
     ↓
[Collect Metrics Activity] → [Send to Kafka Activity] → [Consume & Insert Activity]
                                    ↓                            ↓
                                  [Kafka] ----------------→ [PostgreSQL]
```

## Project Structure

The project is organized into two main implementation approaches:

```
Real-Time Data Streaming/
├── docker-compose.yml        # Docker configuration for infrastructure
├── grafana.json              # Grafana dashboard configuration
├── README.md                 
│
├── temporal/                 # Temporal workflow implementation
│   ├── activities.py         
│   ├── collect.py            
│   ├── start_workflow.py     
│   ├── worker.py            
│   └── workflow.py         
│
└── traditional/              # Traditional Kafka implementation
    ├── consumer.py           
    ├── data_pipeline.py      
    └── producer.py           
```

## Setup Instructions

1. **Clone the repository**:
   ```bash
   git clone <repo-url>
   ```

2. **Start infrastructure services with Docker Compose**:
   ```bash
   docker-compose up -d
   ```
   This will start:
   - Kafka
   - PostgreSQL
   - Temporal Server (for Temporal approach)

3. **Create the PostgreSQL table**:
   ```sql
   CREATE TABLE IF NOT EXISTS "system_Performance" (
     time TIMESTAMP,
     cpu_usage DECIMAL,
     memory_usage DECIMAL,
     cpu_interrupts BIGINT,
     cpu_calls BIGINT,
     memory_used BIGINT,
     memory_free BIGINT,
     bytes_sent BIGINT,
     bytes_received BIGINT,
     disk_usage DECIMAL
   );
   ```

4. **Create the Kafka topic**:
   ```bash
   docker-compose exec kafka kafka-topics --create --topic system_metrics --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

## Approach 1: Traditional Kafka Pipeline

### Components
- `traditional/producer.py`: Collects system metrics and sends to Kafka
- `traditional/consumer.py`: Consumes messages from Kafka and stores in PostgreSQL
- `traditional/data_pipeline.py`: Runs both producer and consumer in a coordinated way

### Running the Traditional Pipeline

1. **Run the data pipeline**:
   ```bash
   python traditional/data_pipeline.py
   ```

2. **Stop the pipeline**:
   - Use Ctrl+C in the terminal where the script is running

## Approach 2: Temporal Workflow Pipeline

### Components
- `temporal/workflow.py`: Defines the workflow logic
- `temporal/activities.py`: Implements the workflow activities
- `temporal/collect.py`: Contains code for collecting system metrics
- `temporal/worker.py`: Registers workflows and activities with Temporal
- `temporal/start_workflow.py`: Initiates the workflow execution

### Running the Temporal Pipeline

1. **Start the Temporal worker** in one terminal:
   ```bash
   python temporal/worker.py
   ```

2. **Start the workflow** in another terminal:
   ```bash
   python temporal/start_workflow.py
   ```

3. **Stop the workflow**:
   - Use the Temporal UI at http://localhost:8080
   - Navigate to your workflow and click "Terminate"
   - Or use the Temporal CLI:
     ```bash
     temporal workflow terminate --workflow-id metrics-collector-workflow
     ```

## Monitoring and Visualization

### Grafana Dashboard
A Grafana dashboard configuration is provided in `grafana.json`. To use it:

1. **Set up Grafana** at http://localhost:3000
2. **Add PostgreSQL as a data source**
3. **Import the dashboard** using the JSON file