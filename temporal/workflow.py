from temporalio import workflow
from datetime import timedelta

# Define a Temporal workflow for continuous system metrics collection and processing
@workflow.defn
class MetricsWorkflow:
    @workflow.run
    async def run(self):
        # Run indefinitely to continuously collect and process metrics
        while True:
            # Step 1: Collect system metrics (CPU, memory, disk usage, etc.)
            # This activity times out after 5 seconds if not completed
            metrics = await workflow.execute_activity(
                "collect_metrics",
                schedule_to_close_timeout=timedelta(seconds=5),
                start_to_close_timeout=timedelta(seconds=5),
            )

            # Step 2: Send the collected metrics to Kafka
            # This activity times out after 5 seconds if not completed
            await workflow.execute_activity(
                "send_to_kafka",
                metrics,
                schedule_to_close_timeout=timedelta(seconds=5),
                start_to_close_timeout=timedelta(seconds=5),
            )

            # Step 3: Consume messages from Kafka and store in PostgreSQL
            # Longer timeout (30s) to allow consumer group setup and processing
            await workflow.execute_activity(
                "consume_and_insert",
                schedule_to_close_timeout=timedelta(seconds=30),
                start_to_close_timeout=timedelta(seconds=30),
            )

            # Brief pause before starting the next cycle
            # This controls the frequency of metrics collection
            await workflow.sleep(0.5)
