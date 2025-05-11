import asyncio
from temporalio.client import Client
from workflow import MetricsWorkflow

async def main():
    # Connect to the Temporal server running on localhost
    client = await Client.connect("localhost:7233")
    
    # Start the metrics collection workflow
    # - Use a consistent ID so we can reference this workflow later
    # - Use the "metrics" task queue which our worker is listening to
    await client.start_workflow(
        MetricsWorkflow.run,
        id="metrics-collector-workflow",
        task_queue="metrics"  
    )

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
