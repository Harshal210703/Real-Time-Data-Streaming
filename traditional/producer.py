from kafka import KafkaProducer
import logging
import json
import time
import psutil
import datetime

# Configure logging with timestamp, level and message format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka connection configuration
bootstrap_servers = 'localhost:9092'
topic = 'system_metrics'

# Initialize the Kafka producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_message(metrics_dict):
    """
    Send a metrics dictionary to the Kafka topic.
    
    Args:
        metrics_dict (dict): Dictionary containing system metrics to be sent to Kafka
    """
    try:
        # Send the metrics to the configured Kafka topic
        producer.send(topic, value=metrics_dict)
        # Ensure the message is sent before continuing (synchronous send)
        producer.flush()
        logging.info(f"Produced: {metrics_dict} to Kafka topic: {topic}")
    except Exception as error:
        logging.error(f"Error sending message to Kafka: {error}")

def collect_metrics():
    """
    Collect current system performance metrics.
    
    Returns:
        dict: Dictionary containing various system metrics
    """
    current_datetime = datetime.datetime.now()
    return {
        "time": str(current_datetime),
        "cpu_usage": str(psutil.cpu_percent()),
        "memory_usage": str(psutil.virtual_memory().percent),
        "cpu_interrupts": str(psutil.cpu_stats().interrupts),
        "cpu_calls": str(psutil.cpu_stats().syscalls),
        "memory_used": str(psutil.virtual_memory().used),
        "memory_free": str(psutil.virtual_memory().free),
        "bytes_sent": str(psutil.net_io_counters().bytes_sent),
        "bytes_received": str(psutil.net_io_counters().bytes_recv),
        "disk_usage": str(psutil.disk_usage('/').percent)
    }

def produce_continuously(interval=1.0):
    """
    Continuously collect and send system metrics to Kafka at the specified interval.
    
    Args:
        interval (float): Time in seconds between metric collections (default: 1.0)
    """
    try:
        logging.info(f"Starting continuous metrics production every {interval} seconds. Press Ctrl+C to stop.")
        while True:
            # Collect metrics
            metrics = collect_metrics()
            # Send to Kafka
            send_message(metrics)
            # Wait for the next collection cycle
            time.sleep(interval)
    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")
    finally:
        producer.close()
        logging.info("Kafka producer connection closed.")
