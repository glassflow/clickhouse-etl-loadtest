from glassflow_clickhouse_etl import Pipeline
from src.generate_events import generate_events_with_duplicates
import multiprocessing
from typing import List, Dict
from src.utils.logger import log


def publish_events(pipeline: Pipeline, generator_schema, num_records, variant_config):    
    gen_stats = generate_events_with_duplicates(
        source_config=pipeline.config.source,
        duplication_rate=variant_config["duplication_rate"],
        num_records=num_records,        
        rps=20000,
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

def publish_to_kafka(pipeline: Pipeline, generator_schema: str, variant_config: Dict) -> List[Dict]:
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

    publish_stats = {
        "total_generated": sum(stats["total_generated"] for stats in results),
        "total_duplicates": sum(stats["total_duplicates"] for stats in results),
        "num_records": sum(stats["num_records"] for stats in results),
        "time_taken_publish_ms": max(stats["time_taken_ms"] for stats in results)
    }
    return publish_stats
