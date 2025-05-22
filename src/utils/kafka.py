import base64
import tempfile
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    KafkaError,
    KafkaException
)
from glassflow_clickhouse_etl import models
from src.utils.logger import log


def create_kafka_admin_client(source_config: models.SourceConfig):
    """Create a Kafka admin client"""
    if source_config.connection_params.root_ca:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as ca_cert_file:
            # base64 decode the root ca
            ca_cert_file.write(base64.b64decode(source_config.connection_params.root_ca).decode("utf-8"))
            ca_cert_path = ca_cert_file.name
    else:
        ca_cert_path = None

    if source_config.connection_params.brokers[0] == "kafka:9094":
        brokers = ["localhost:9093"]
    else:
        brokers = source_config.connection_params.brokers
    
    return AdminClient({
            "bootstrap.servers": ",".join(brokers),
            "security.protocol": source_config.connection_params.protocol.value,
            "sasl.mechanisms": source_config.connection_params.mechanism.value,
            "sasl.username": source_config.connection_params.username,
            "sasl.password": source_config.connection_params.password,        
            "ssl.ca.location": ca_cert_path        
        }
    )

def create_topics_if_not_exists(source_config: models.SourceConfig):
    """Create topics in Kafka"""
    admin_client = create_kafka_admin_client(source_config)

    # Create topic configuration
    for topic_config in source_config.topics:
        topic_name = topic_config.name         
        # First check if topic exists            
        topic_config = {
            "message.timestamp.type": "LogAppendTime",         
        }
        topic = NewTopic(
            topic_name,
            num_partitions=3,
            replication_factor=1,
            config=topic_config
        )
        admin_client.poll(3)
        
        fs = admin_client.create_topics([topic])

        for topic, f in fs.items():
            try:
                f.result()
                log(
                    message=f"Topic [italic u]{topic}[/italic u]",
                    status="Created",
                    is_success=True,
                    component="Kafka",
                )
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    log(
                        message=f"Topic [italic u]{topic}[/italic u]",
                        status="Already exists",
                        is_success=True,
                        component="Kafka",
                    )
                else:
                    raise Exception(f"❌ Failed to create topic {topic}: {e}")
            except Exception as e:
                err_msg = f"❌ Failed to create topic {topic}: {e}"
                log(
                    message=err_msg,
                    status="Failed",
                    is_failure=True,
                    component="Kafka",
                )
                raise Exception(err_msg)


def cleanup_kafka(source_config: models.SourceConfig):
    """Delete all Kafka topics that begin with 'load_'"""
    try:
        # Create Kafka admin client with the same configuration as used in the project
        admin_client = create_kafka_admin_client(source_config)

        # Get all topics
        metadata = admin_client.list_topics(timeout=10)
        topics = [topic.topic for topic in metadata.topics.values()]
        
        # Filter topics that begin with 'load_'
        load_topics = [topic for topic in topics if topic.startswith('load_')]
        
        if load_topics:
            # Delete the filtered topics
            fs = admin_client.delete_topics(load_topics, operation_timeout=30)
            
            # Wait for all operations to complete
            for topic, future in fs.items():
                try:
                    future.result()  # Wait for the operation to complete
                    log(
                        message=f"Cleanup: Deleted Topic [italic u]{topic}[/italic u]",
                        status="Deleted",
                        is_success=True,
                        component="Kafka",
                    )
                except KafkaError as e:
                    log(
                        message=f"Error deleting topic {topic}",
                        status=str(e),
                        is_failure=True,
                        component="Kafka",
                    )
        else:
            log(
                message="No topics to delete",
                status="Skipped",
                is_success=True,
                component="Kafka",
            )
            
    except KafkaError as e:
        log(
            message="Error deleting topics",
            status=str(e),
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