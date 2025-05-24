import pandas as pd
import json
import argparse
from typing import List
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from src.utils.metrics import TestResultsHandler
import traceback
console = Console(width=80)

def format_value(key: str, value) -> str:
    """Format value based on its type and key"""
    if isinstance(value, (float, int)) and 'ms' in key:
        return f"{value/1000:.2f}s"  # Convert ms to seconds        
    elif isinstance(value, (int)):
        return str(value)
    return str(value)

def get_display_name(key: str) -> str:
    """Convert internal key to user-friendly name"""
    display_names = {
        # Parameters
        'variant_id': 'Variant ID',
        'param_max_batch_size': 'Max Batch Size',
        # Results
        'result_num_records': 'Number of Records',
        'result_time_taken_publish_ms': 'Time to Publish',
        'result_rps_achieved': 'Source RPS in Kafka',
        'result_time_taken_ms': 'Time to Process',
        'result_avg_latency_ms': 'Average Latency',
        'result_lag_ms': 'Lag',
        'glassflow_rps': 'GlassFlow RPS'
    }
    return display_names.get(key, key)

def display_variant_results(row):
    """Display a single variant's results in a formatted way"""
    # Prepare parameters section
    params = {
        'Variant ID': row['variant_id'],
        'Max Batch Size': row['param_max_batch_size'],
        'Duplication Rate': row['param_duplication_rate'],
        'Deduplication Window': row['param_deduplication_window'],
        'Max Delay Time': row['param_max_delay_time']
    }
    
    # Prepare results section
    results = {
        'Success': f"{row['result_success']}",
        'Number of Records': f"{round(row['result_num_records'] / 1_000_000, 2)}M",
        'Time to Publish': f"{round(row['result_time_taken_publish_ms']/ 1000, 2)} s",
        'Source RPS in Kafka': f"{round(row['result_rps_achieved'])} records/s",
        'GlassFlow RPS': f"{round(row['result_glassflow_rps'])} records/s",
        'Time to Process': f"{round(row['result_time_taken_ms']/ 1000, 4)} s",
        'Average Latency': f"{round(row['result_avg_latency_ms']/ 1000, 4)} s",
        'Lag': f"{round(row['result_lag_ms']/ 1000, 4)} s"
    }
    
    # Create the output structure
    output = {
        'Parameters': params,
        'Results': results
    }
    
    # Convert to JSON with proper formatting
    json_output = json.dumps(output, indent=2)
    
    # Create a panel with the JSON output
    panel = Panel(
        Text(json_output, style="white"),
        title=f"Test Results for {row['variant_id']} - {'Success' if row['result_success'] else 'Failed'}",
        border_style="blue"
    )
    
    console.print(panel)
    console.print()  # Add a blank line between variants

def display_results(results: List):
    # Display results for each variant
    console.print("[bold blue]Test Results:[/bold blue]")
    console.print(f"[bold green]Total Variants: {len(results)}[/bold green]")
    for row in results:
        display_variant_results(row)

def main():
    parser = argparse.ArgumentParser(description='Analyze load test results from a CSV file')
    parser.add_argument('--results-file', required=True,
                       help='Path to the results CSV file (e.g., results/test_id_results.csv)')
    args = parser.parse_args()

    try:
        handler = TestResultsHandler(args.results_file)
        results = handler.read_validated_results()
        display_results(results)
    except FileNotFoundError:
        console.print(f"[red]Error: Results file not found: {args.results_file}[/red]")
    except Exception as e:
        console.print(f"[red]Error analyzing results: {str(e)}[/red]")
        print(traceback.format_exc())   

if __name__ == "__main__":
    main()
