from src.pre_process import setup_pipeline
from glassflow_clickhouse_etl import Pipeline
from src.generate_events import generate_events_with_duplicates
import time
import multiprocessing
from typing import List, Dict
from rich.console import Console
from rich.panel import Panel
from src.utils.logger import log
from src.utils.clickhouse import read_clickhouse_table_size, create_clickhouse_client
from src.utils.pipeline import GlassFlowPipeline
from src.utils.metrics import TestResultModel

console = Console(width=140)

def publish_events(pipeline: Pipeline, generator_schema, num_records, variant_config):    
    gen_stats = generate_events_with_duplicates(
        source_config=pipeline.config.source,
        duplication_rate=variant_config["duplication_rate"],
        num_records=num_records,        
        rps=variant_config["records_per_second"],
        bulk_size=variant_config["max_batch_size"],
        generator_schema=generator_schema,
    )
    return gen_stats

def publish_events_worker(args):
    """Worker function that will be run in a separate process"""
    pipeline_config, generator_schema, num_records, variant_config, process_id = args
    # Create a new pipeline instance for this process
    pipeline = Pipeline(config=pipeline_config)
    log(
        message=f"Process {process_id} started publishing events",
        status="Started",
        is_success=True,
        component="GlassGen"
    )
    stats = publish_events(pipeline, generator_schema, num_records, variant_config)
    log(
        message=f"Process {process_id} finished publishing events",
        status="Finished",
        is_success=True,
        component="GlassGen"
    )
    return stats

def run_parallel_publishers(pipeline: Pipeline, generator_schema: str, variant_config: Dict) -> List[Dict]:
    """Run multiple publish_events processes in parallel"""
    # Prepare arguments for each process
    num_processes = variant_config["num_processes"]
    total_records = variant_config["total_records"]
    
    # Calculate base records per process and remainder
    base_records = total_records // num_processes
    remainder = total_records % num_processes
    
    # Create process arguments with adjusted record counts
    process_args = []
    for i in range(num_processes):        
        # Give all remainder records to the first process
        num_records = base_records + (remainder if i == 0 else 0)
        process_args.append((pipeline.config, generator_schema, num_records, variant_config, i))
    
    # Create a pool of workers
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Map the work across the processes
        results = pool.map(publish_events_worker, process_args)
    
    return results

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

def run_variant(pipeline_config_path: str, generator_schema: str, variant_id: str, variant_config: dict, pipeline: GlassFlowPipeline, test_result: TestResultModel):
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
    gen_stats_list = run_parallel_publishers(pipeline, generator_schema, variant_config)
    
    # Aggregate stats from all processes
    total_generated = sum(stats["total_generated"] for stats in gen_stats_list)
    total_duplicates = sum(stats["total_duplicates"] for stats in gen_stats_list)
    num_records = sum(stats["num_records"] for stats in gen_stats_list)
    time_taken_ms = max(stats["time_taken_ms"] for stats in gen_stats_list)
    
    test_result.result_total_generated = total_generated
    test_result.result_total_duplicates = total_duplicates
    test_result.result_num_records = num_records
    test_result.result_num_processes = variant_config["num_processes"]
    test_result.result_time_taken_publish_ms = time_taken_ms
    test_result.result_rps_achieved = round((num_records / time_taken_ms) * 1000)
    
    console.print(Panel(
        "[green]Data published successfully[/green]",
        title="✅ Publication Complete",
        border_style="green"
    ))
    
    # Wait for records to be available in ClickHouse
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
    test_result.result_avg_latency_ms = time_taken_complete_ms / num_records
    test_result.result_lag_ms = round((record_reading_end_time - record_reading_start_time) * 1000)
    test_result.result_glassflow_rps = round((num_records / time_taken_complete_ms) * 1000)
    
    return test_result

