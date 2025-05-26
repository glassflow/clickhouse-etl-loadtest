from src.pre_process import setup_pipeline
import time
from rich.console import Console
from rich.panel import Panel
from src.utils.logger import log
from src.utils.clickhouse import read_clickhouse_table_size, create_clickhouse_client
from src.utils.pipeline import GlassFlowPipeline
from src.utils.metrics import TestResultModel
from src.utils.publish import publish_to_kafka

console = Console(width=140)

def wait_for_records(clickhouse_client, pipeline_config, n_records_before, total_generated, max_retries=30, retry_interval=10):
    """Wait for records to be available in ClickHouse with retries"""
    retries = 0
    last_percentage = 0    
    while retries < max_retries:
        n_records_after = read_clickhouse_table_size(
            pipeline_config.sink, clickhouse_client
        )
        added_records = n_records_after - n_records_before
        
        if added_records == total_generated:        
            return True        
        percentage = round(added_records/total_generated*100)
        # only log if percentage has changed by atleast 5
        if abs(percentage - last_percentage) >= 5:
            message = f"Waiting for records to be available... (attempt {retries + 1}/{max_retries}) Expected: {total_generated}, Found: {added_records} ({percentage}%)"
            log(
                message=message,
                status="Waiting",
                is_warning=True,
                component="Pipeline"
            )
            last_percentage = percentage
        time.sleep(retry_interval)
        retries += 1
    
    console.print(Panel(
        f"[red]Timeout waiting for records[/red]\n"
        f"Expected: {total_generated}, Found: {added_records}",
        title="❌ Timeout",
        border_style="red"
    ))
    return False

def run_variant(pipeline_config_path: str, event_schema: str, variant_id: str, variant_config: dict, pipeline: GlassFlowPipeline, test_result: TestResultModel):
    """Run a single variant of the load test"""
    # Set up pipeline with test configuration
    pipeline = setup_pipeline(variant_id, pipeline_config_path, variant_config, pipeline)
    
    log(
        message=f"Pipeline started: {pipeline.get_running_pipeline()}",
        status="Started",
        is_success=True,
        component="Pipeline"
    )
    
    clickhouse_client = create_clickhouse_client(pipeline.config.sink)
    n_records_before = read_clickhouse_table_size(
        pipeline.config.sink, clickhouse_client
    )
    start_time = time.time()
    
    # Run multiple publishers in parallel
    publish_stats = publish_to_kafka(pipeline, event_schema, variant_config)
    # update
    test_result.result_num_processes = variant_config["num_processes"]
    test_result.result_total_generated = publish_stats['total_generated']
    test_result.result_total_duplicates = publish_stats['total_duplicates']
    test_result.result_num_records = publish_stats['num_records']    
    test_result.result_time_taken_publish_ms = publish_stats['time_taken_publish_ms']
    test_result.result_kafka_ingestion_rps = publish_stats['kafka_ingestion_rps']
    
    console.print(Panel(
        "[green]Data published successfully[/green]",
        title="✅ Publication Complete",
        border_style="green"
    ))
    
    # Wait for records to be available in ClickHouse
    total_generated = publish_stats['total_generated']

    record_reading_start_time = time.time()
    records_available = wait_for_records(
        clickhouse_client=clickhouse_client,
        pipeline_config=pipeline.config,
        n_records_before=n_records_before,
        total_generated=total_generated,
        max_retries=1000,
        retry_interval=5
    )
    record_reading_end_time = time.time()
    if not records_available:
        success = False
    else:
        console.print(Panel(
            f"[green]Excpeted records available in ClickHouse: Found {total_generated} records[/green]",
            title="✅ Success",
            border_style="green"
        ))
        success = True
    
    time_taken_complete_ms = round((time.time() - start_time) * 1000)
    test_result.result_success = success
    test_result.result_time_taken_ms = time_taken_complete_ms

    # average latency 
    test_result.result_avg_latency_ms = time_taken_complete_ms / publish_stats['num_records']
    test_result.result_lag_ms = round((record_reading_end_time - record_reading_start_time) * 1000)
    test_result.result_glassflow_rps = round((publish_stats['num_records'] / time_taken_complete_ms) * 1000)
    
    return test_result

