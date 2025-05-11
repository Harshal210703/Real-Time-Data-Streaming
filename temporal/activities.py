from temporalio import activity
import psutil
import datetime
import asyncio

# Add parent directory to sys.path to allow importing the traditional package
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from traditional.producer import send_message
from traditional.consumer import consume

# Activity 1: Collects current system metrics
@activity.defn
async def collect_metrics():
    """
    Collects various system performance metrics including CPU, memory,
    network, and disk usage. Returns a dictionary of string metrics.
    """
    current_datetime = datetime.datetime.now()
    return {
        "time": str(current_datetime),
        "cpu_usage": str(psutil.cpu_percent()),  # CPU usage percentage
        "memory_usage": str(psutil.virtual_memory().percent),  # RAM usage percentage
        "cpu_interrupts": str(psutil.cpu_stats().interrupts),  # CPU hardware interrupts since boot
        "cpu_calls": str(psutil.cpu_stats().syscalls),  # System calls since boot
        "memory_used": str(psutil.virtual_memory().used),  # RAM used in bytes
        "memory_free": str(psutil.virtual_memory().free),  # RAM free in bytes
        "bytes_sent": str(psutil.net_io_counters().bytes_sent),  # Network bytes sent since boot
        "bytes_received": str(psutil.net_io_counters().bytes_recv),  # Network bytes received since boot
        "disk_usage": str(psutil.disk_usage('/').percent)  # Disk usage percentage
    }

# Activity 2: Sends the metrics to Kafka
@activity.defn
async def send_to_kafka(metrics: dict):
    """
    Takes a dictionary of metrics and sends it to Kafka.
    Uses run_in_executor to run the non-async send_message function
    without blocking the event loop.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, send_message, metrics)
    return True

# Activity 3: Consumes messages from Kafka and stores in PostgreSQL
@activity.defn
async def consume_and_insert():
    """
    Consumes messages from Kafka and inserts them into PostgreSQL.
    Uses run_in_executor to run the non-async consume function
    without blocking the event loop.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, consume)
    return True
