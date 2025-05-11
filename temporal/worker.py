import asyncio
import sys
import os

# Add the parent directory to the Python path 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from temporalio.client import Client
from temporalio.worker import Worker

from workflow import MetricsWorkflow
from activities import collect_metrics, send_to_kafka, consume_and_insert

async def main():
    # Connect to the Temporal server
    client = await Client.connect("localhost:7233")

    # Create a Temporal worker that:
    # - Listens on the "metrics" task queue
    # - Handles the MetricsWorkflow
    # - Executes the three activities defined in activities.py
    worker = Worker(
        client,
        task_queue="metrics",
        workflows=[MetricsWorkflow],
        activities=[
            collect_metrics,
            send_to_kafka,
            consume_and_insert
        ]
    )

    # Start the worker - this will block and process tasks indefinitely
    # until the process is terminated
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
