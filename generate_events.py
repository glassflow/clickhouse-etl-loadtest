from glassflow_clickhouse_etl.models import SourceConfig
import glassgen 
import json

def generate_events_with_duplicates(
    source_config: SourceConfig,
    generator_schema: str,
    duplication_rate: float = 0.1,
    num_records: int = 10000,
    rps: int = 1000,
    bulk_size: int = 50000,
):
    """Generate events with duplicates

    Args:
        source_config (SourceConfig): Source configuration
        duplication_rate (float, optional): Duplication rate. Defaults to 0.1.
        num_records (int, optional): Number of records to generate. Defaults to 10000.
        rps (int, optional): Records per second. Defaults to 1000.
        generator_schema (str, optional): Path to generator schema.
    """
    glassgen_config = {
        "generator": {
            "num_records": num_records,
            "rps": rps,
            "bulk_size": bulk_size
        }
    }
    if source_config.topics[0].deduplication.enabled:
        duplication_config = {
            "duplication": {
                "enabled": True,
                "ratio": duplication_rate,
                "key_field": source_config.topics[0].deduplication.id_field,
                "time_window": source_config.topics[0].deduplication.time_window,
            }
        }
    else:
        duplication_config = {"duplication": None}

    glassgen_config["generator"]["event_options"] = duplication_config
    schema = json.load(open(generator_schema))
    glassgen_config["schema"] = schema

    if source_config.connection_params.brokers[0] == "kafka:9094":
        brokers = ["localhost:9093"]
    else:
        brokers = source_config.connection_params.brokers

    glassgen_config["sink"] = {
        "type": "kafka",
        "params": {
            "bootstrap.servers": ",".join(brokers),
            "topic": source_config.topics[0].name,
            "security.protocol": source_config.connection_params.protocol,
            "sasl.mechanism": source_config.connection_params.mechanism,
            "sasl.username": source_config.connection_params.username,
            "sasl.password": source_config.connection_params.password,
        },
    }
    return glassgen.generate(config=glassgen_config)
