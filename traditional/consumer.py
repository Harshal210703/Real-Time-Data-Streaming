from kafka import KafkaConsumer
import psycopg2
import logging
import json
import sys
import time

# Configure logging with timestamp, level and message format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def consume():
    """
    Consumes system metrics messages from Kafka and stores them in PostgreSQL.
    
    The function connects to PostgreSQL, sets up a Kafka consumer, processes messages,
    and inserts the data into the system_Performance table. It will exit after processing
    a maximum number of messages or when no new messages are available for a timeout period.
    """
    try:
        # Establish connection to PostgreSQL database
        sql_conn = psycopg2.connect(
            host="localhost",      
            port="5432",
            database="postgres",
            user="postgres",
            password="postgres"
        )
        sql_cursor = sql_conn.cursor()

        # Initialize Kafka consumer with configurations:
        # - Subscribe to 'system_metrics' topic
        # - Latest offset means only consume new messages
        # - Consumer group allows multiple consumers to coordinate
        # - JSON deserializer to parse message values
        # - 10-second timeout ensures the consumer doesn't run indefinitely
        consumer = KafkaConsumer(
            'system_metrics',
            bootstrap_servers='localhost:9092',  
            auto_offset_reset='latest',
            group_id='system-metrics-group-v3',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # 10-second timeout allows consumer to exit when no messages
        )

        logging.info("Kafka Consumer started. Listening for messages...")
        
        # Tracking variables for message processing
        messages_processed = 0
        max_messages = 10  # Process at most 10 messages per invocation
        
        try:
            # Process each message from the Kafka topic
            for message in consumer:
                # Extract the message value (the metrics dictionary)
                data_dict = message.value
                
                # Convert dictionary to tuple for database insertion
                data = (
                    data_dict.get("time"),
                    data_dict.get("cpu_usage"),
                    data_dict.get("memory_usage"),
                    data_dict.get("cpu_interrupts"),
                    data_dict.get("cpu_calls"),
                    data_dict.get("memory_used"),
                    data_dict.get("memory_free"),
                    data_dict.get("bytes_sent"),
                    data_dict.get("bytes_received"),
                    data_dict.get("disk_usage")
                )

                # Validate the data is complete before inserting
                if None in data:
                    logging.warning(f"Incomplete or invalid message format: {message.value}")
                    continue

                # SQL query for inserting the metrics into the database
                insert_query = """
                    INSERT INTO "system_Performance" (
                        time, cpu_usage, memory_usage, cpu_interrupts,
                        cpu_calls, memory_used, memory_free,
                        bytes_sent, bytes_received, disk_usage
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                try:
                    # Execute the SQL query with the metrics data
                    sql_cursor.execute(insert_query, data)
                    # Commit the transaction to save the data
                    sql_conn.commit()
                    logging.info(f"Inserted data into PostgreSQL: {data}")
                    messages_processed += 1
                    
                    # Exit after processing max_messages to avoid blocking too long
                    if messages_processed >= max_messages:
                        logging.info(f"Reached maximum messages ({max_messages}), exiting consumer")
                        break
                        
                except Exception as e:
                    logging.error(f"Error inserting into DB: {e}")
                    # Rollback transaction on error to maintain data integrity
                    sql_conn.rollback()
                    
        except StopIteration:
            # Consumer timeout occurred (no messages within timeout period)
            logging.info("No more messages available within timeout period")
            
        logging.info(f"Consumer finished. Processed {messages_processed} messages.")

    except KeyboardInterrupt:
        # Handle manual interruption gracefully
        logging.info("Shutting down consumer due to KeyboardInterrupt.")

    except Exception as e:
        # Log critical startup errors
        logging.error(f"Startup error: {e}")
        sys.exit(1)

    finally:
        # Ensure all resources are properly closed regardless of how the function exits
        try:
            if 'sql_cursor' in locals():
                sql_cursor.close()
            if 'sql_conn' in locals():
                sql_conn.close()
            if 'consumer' in locals():
                consumer.close()
                logging.info("Consumer closed successfully")
        except Exception as close_err:
            logging.warning(f"Error during cleanup: {close_err}")
