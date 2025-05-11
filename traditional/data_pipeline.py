import time
import psutil
import datetime
from traditional.producer import send_message  # Kafka producer function
from traditional.consumer import consume       # Kafka consumer function
import threading
import logging

# Configure logging for both producer and consumer threads
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_producer():
    """
    Producer thread function that continuously collects system metrics
    and sends them to Kafka using the send_message function.
    
    This function runs in an infinite loop with a small delay between iterations,
    capturing real-time system performance data.
    """
    while True:
        try:
            # Capture the current timestamp
            current_datetime = datetime.datetime.now()

            # Collect comprehensive system metrics using psutil
            cpu_usage = psutil.cpu_percent()  # Overall CPU utilization as percentage
            memory_usage = psutil.virtual_memory().percent  # RAM usage percentage
            cpu_interrupts = psutil.cpu_stats().interrupts  # Hardware interrupts since boot
            cpu_calls = psutil.cpu_stats().syscalls  # System calls since boot
            memory_used = psutil.virtual_memory().used  # Used RAM in bytes
            memory_free = psutil.virtual_memory().free  # Available RAM in bytes
            bytes_sent = psutil.net_io_counters().bytes_sent  # Network bytes sent
            bytes_received = psutil.net_io_counters().bytes_recv  # Network bytes received
            disk_usage = psutil.disk_usage('/').percent  # Root disk usage percentage

            # Format metrics into a structured dictionary for Kafka
            message = {
                "time": str(current_datetime),
                "cpu_usage": str(cpu_usage),
                "memory_usage": str(memory_usage),
                "cpu_interrupts": str(cpu_interrupts),
                "cpu_calls": str(cpu_calls),
                "memory_used": str(memory_used),
                "memory_free": str(memory_free),
                "bytes_sent": str(bytes_sent),
                "bytes_received": str(bytes_received),
                "disk_usage": str(disk_usage)
            }

            # Send the metrics to the Kafka topic
            send_message(message)

            # Wait briefly before next collection to avoid overwhelming the system
            # This controls the data collection frequency (2 samples per second)
            time.sleep(0.5)

        except Exception as e:
            logging.error(f"Error in producer thread: {e}")

def run_consumer():
    """
    Consumer thread function that continuously processes messages from Kafka
    and stores them in PostgreSQL.
    
    This function runs in an infinite loop, calling the consume() function
    which handles the Kafka consumer logic and database operations.
    """
    while True:
        try:
            # Process a batch of messages from Kafka and store in PostgreSQL
            # The consume() function handles all the consumer logic including:
            # - Connecting to Kafka and consuming messages
            # - Transforming message data
            # - Storing metrics in the PostgreSQL database
            # - Proper cleanup of resources
            consume()

            # Small delay to prevent excessive CPU usage from tight polling
            time.sleep(0.1)

        except Exception as e:
            logging.error(f"Error in consumer thread: {e}")
            # Continue running even if an error occurs

# Main execution: start both producer and consumer threads
if __name__ == "__main__":
    # Create producer thread that will collect metrics and send to Kafka
    producer_t = threading.Thread(target=run_producer, name="ProducerThread")
    producer_t.daemon = True  # Thread will exit when main program exits

    # Create consumer thread that will receive from Kafka and store in PostgreSQL
    consumer_t = threading.Thread(target=run_consumer, name="ConsumerThread")
    consumer_t.daemon = True  # Thread will exit when main program exits

    logging.info("Starting real-time data streaming pipeline")
    
    # Start both threads
    producer_t.start()
    consumer_t.start()

    try:
        # Keep main thread alive while worker threads run
        # Using join() with no timeout means wait indefinitely
        producer_t.join()
        consumer_t.join()
    except KeyboardInterrupt:
        # Allow graceful exit with Ctrl+C
        logging.info("Data pipeline shutdown initiated by user")
