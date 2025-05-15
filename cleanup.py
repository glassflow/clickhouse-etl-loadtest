from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError
from utils import log
import clickhouse_connect
import base64
from utils import stop_pipeline_if_running


def cleanup_kafka():
    """Delete all Kafka topics that begin with 'load_'"""
    try:
        # Create Kafka admin client with the same configuration as used in the project
        admin_client = KafkaAdminClient(
            bootstrap_servers=["localhost:9093"],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="admin",
            sasl_plain_password="admin-secret",
        )

        # Get all topics
        topics = admin_client.list_topics()
        
        # Filter topics that begin with 'load_'
        load_topics = [topic for topic in topics if topic.startswith('load_')]
        
        if load_topics:
            # Delete the filtered topics
            admin_client.delete_topics(load_topics)
            for topic in load_topics:
                log(
                    message=f"Cleanup: Deleted Topic [italic u]{topic}[/italic u]",
                    status="Deleted",
                    is_success=True,
                    component="Kafka",
                )
        else:
            log(
                message="No topics to delete",
                status="Skipped",
                is_success=True,
                component="Kafka",
            )
            
    except UnknownTopicOrPartitionError:
        log(
            message="Error deleting topics",
            status="Topics do not exist",
            is_warning=True,
            component="Kafka",
        )
    except Exception as e:
        log(
            message="Error cleaning up Kafka topics",
            status=str(e),
            is_failure=True,
            component="Kafka",
        )
    finally:
        if 'admin_client' in locals():
            admin_client.close()

def cleanup_clickhouse():
    """Delete all ClickHouse tables that begin with 'load_'"""
    try:
        # Create ClickHouse client with the same configuration as used in the project
        client = clickhouse_connect.get_client(
            host="localhost",
            username="default",
            password=base64.b64decode("c2VjcmV0").decode("utf-8"),  # 'secret' in base64
            database="default",
            port=8123,
        )

        # Get all tables in the default database
        result = client.query("SHOW TABLES")
        tables = [row[0] for row in result.result_rows]  # Extract table names from result rows
        
        # Filter tables that begin with 'load_'
        load_tables = [table for table in tables if table.startswith('load_')]
        
        if load_tables:
            # Drop each table
            for table in load_tables:
                client.command(f"DROP TABLE IF EXISTS {table}")
                log(
                    message=f"Cleanup: Deleted Table [italic u]{table}[/italic u]",
                    status="Deleted",
                    is_success=True,
                    component="Clickhouse",
                )
        else:
            log(
                message="No tables to delete",
                status="Skipped",
                is_success=True,
                component="Clickhouse",
            )
            
    except Exception as e:
        log(
            message="Error cleaning up ClickHouse tables",
            status=str(e),
            is_failure=True,
            component="Clickhouse",
        )
    finally:
        if 'client' in locals():
            client.close()

def cleanup_pipeline():
    stop_pipeline_if_running()

if __name__ == "__main__":
    cleanup_kafka()
    cleanup_clickhouse()
    cleanup_pipeline()