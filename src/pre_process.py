from src import utils
import json
from glassflow_clickhouse_etl.models import PipelineConfig

def pre_process_kafka_clickhouse(pipeline_config: PipelineConfig):
    clickhouse_client = utils.create_clickhouse_client(pipeline_config.sink)
    if pipeline_config.join.enabled:
        join_key = pipeline_config.join.sources[0].join_key
    else:
        join_key = None
    utils.create_table_if_not_exists(pipeline_config.sink, clickhouse_client, join_key)
    utils.create_topics_if_not_exists(pipeline_config.source)


def update_pipeline_config(config, variant_id, variant_config):
    # Update pipeline configuration with new load test ID
    dedup_window = variant_config["deduplication_window"]
    max_batch_size = variant_config["max_batch_size"]
    max_delay_time = variant_config["max_delay_time"]
    #variant_config 
    config["pipeline_id"] = variant_id
    config["source"]["topics"][0]["name"] = f"{variant_id}"
    config["sink"]["table"] = f"{variant_id}"
    
    # Update all source_ids in table_mapping
    for mapping in config["sink"]["table_mapping"]:
        mapping["source_id"] = f"{variant_id}"
    
    # update the deduplication_window
    config["source"]["topics"][0]["deduplication"]["time_window"] = dedup_window
    config["sink"]["max_batch_size"] = max_batch_size
    config["sink"]["max_delay_time"] = max_delay_time
    return config

def setup_pipeline(variant_id, pipeline_config_path, variant_config):

    # Load and update pipeline configuration
    pipeline_config = json.load(open(pipeline_config_path))
    
    # Update configuration with new load test ID
    updated_config = update_pipeline_config(pipeline_config, variant_id, variant_config)
    pipeline_config = utils.load_conf(updated_config)    
    # pre process the pipeline config to create the table and topics
    pre_process_kafka_clickhouse(pipeline_config)

    # create the pipeline
    # remove any existing pipeline and create a new one
    utils.stop_pipeline_if_running()    
    # create the pipeline
    pipeline = utils.create_pipeline(pipeline_config)
    return pipeline
