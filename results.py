import pandas as pd
import csv
import argparse
from rich.console import Console
from rich.table import Table

console = Console(width=140)

def analyze_results(csv_file: str):
    """Analyze the test results from the given CSV file"""
    # Read CSV using csv package
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        # Get header row
        headers = next(csv_reader)
        
        # Create a table to display column names
        table = Table(title="CSV Columns", show_header=True, header_style="bold magenta")
        table.add_column("Index", style="cyan")
        table.add_column("Column Name", style="green")
        
        # Add each column name to the table
        for idx, header in enumerate(headers):
            table.add_row(str(idx), header)
        
        console.print(table)

    # Continue with pandas for analysis
    df = pd.read_csv(csv_file)

    # Define important columns and their display names
    important_columns = {
        'variant_id': 'Variant ID',
        'duration_sec': 'Time taken for the test (sec)',
        'result_num_records': 'Number of records',
        'result_time_taken_publish_ms': 'Time taken to publish (ms)',
        'result_rps_achieved': 'RPS Achieved',    
        'result_time_taken_ms': 'Time taken to process records (ms)',    
        'result_avg_latency_ms': 'Average Latency (ms)',
        'result_success': 'Success',
    }

    # Create a table for important columns
    results_table = Table(title="Test Results", show_header=True, header_style="bold magenta")
    for col in important_columns.values():
        results_table.add_column(col, style="cyan")

    # Add data rows
    for _, row in df.iterrows():
        results_table.add_row(
            str(row['variant_id']),
            str(round(row['duration_sec'], 2)),
            str(row['result_num_records']),
            str(row['result_time_taken_publish_ms']),
            str(row['result_rps_achieved']),   
            str(row['result_time_taken_ms']),             
            str(round(row['result_avg_latency_ms'], 2)),
            "✅" if row['result_success'] else "❌"
        )

    console.print(results_table)

def main():
    parser = argparse.ArgumentParser(description='Analyze load test results from a CSV file')
    parser.add_argument('--results-file', required=True,
                       help='Path to the results CSV file (e.g., results/test_id_results.csv)')
    args = parser.parse_args()

    try:
        analyze_results(args.results_file)
    except FileNotFoundError:
        console.print(f"[red]Error: Results file not found: {args.results_file}[/red]")
    except Exception as e:
        console.print(f"[red]Error analyzing results: {str(e)}[/red]")

if __name__ == "__main__":
    main()
