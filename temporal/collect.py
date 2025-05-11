from temporalio import activity
import psutil
import datetime

# Define a Temporal activity for collecting system performance metrics
@activity.defn
async def collect_metrics():
    """
    Collects comprehensive system performance metrics in real-time.
    
    Returns:
        dict: A dictionary containing various system metrics with string values:
            - time: Current timestamp
            - cpu_usage: Current CPU utilization percentage
            - memory_usage: Current RAM utilization percentage
            ...
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
