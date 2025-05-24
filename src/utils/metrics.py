# storing metrics for the load test in the file 

from datetime import datetime
from typing import Optional, Dict, List
from pydantic import BaseModel, Field
import csv
import os
from pathlib import Path
from rich.table import Table
from rich.console import Console


class TestResultModel(BaseModel):
    """Pydantic model representing a test result"""
    # Test identification
    test_id: str
    variant_id: str
    timestamp: datetime = Field(default_factory=datetime.now)
        
    # Test duration
    duration_sec: float
    
    # Test parameters
    param_num_processes: int
    param_records_per_second: int
    param_total_records: int
    param_duplication_rate: float
    param_deduplication_window: str
    param_max_batch_size: int
    param_max_delay_time: str
    
    # Test results
    result_total_generated: Optional[int] = None
    result_total_duplicates: Optional[int] = None
    result_num_records: Optional[int] = None
    result_num_processes: Optional[int] = None
    result_time_taken_publish_ms: Optional[float] = None
    result_rps_achieved: Optional[float] = None
    result_success: Optional[bool] = None
    result_time_taken_ms: Optional[float] = None
    result_avg_latency_ms: Optional[float] = None
    result_lag_ms: Optional[float] = None
    result_glassflow_rps: Optional[float] = None
    
    def to_csv_row(self) -> dict:
        """Convert the model to a dictionary suitable for CSV writing"""
        return {
            'test_id': self.test_id,
            'variant_id': self.variant_id,
            'timestamp': self.timestamp.isoformat(),            
            'duration_sec': str(self.duration_sec),
            'param_num_processes': str(self.param_num_processes),
            'param_records_per_second': str(self.param_records_per_second),
            'param_total_records': str(self.param_total_records),
            'param_duplication_rate': str(self.param_duplication_rate),
            'param_deduplication_window': self.param_deduplication_window,
            'param_max_batch_size': str(self.param_max_batch_size),
            'param_max_delay_time': self.param_max_delay_time,
            'result_total_generated': str(self.result_total_generated) if self.result_total_generated is not None else '',
            'result_total_duplicates': str(self.result_total_duplicates) if self.result_total_duplicates is not None else '',
            'result_num_records': str(self.result_num_records) if self.result_num_records is not None else '',
            'result_num_processes': str(self.result_num_processes) if self.result_num_processes is not None else '',
            'result_time_taken_publish_ms': str(self.result_time_taken_publish_ms) if self.result_time_taken_publish_ms is not None else '',
            'result_rps_achieved': str(self.result_rps_achieved) if self.result_rps_achieved is not None else '',
            'result_success': str(self.result_success) if self.result_success is not None else '',
            'result_time_taken_ms': str(self.result_time_taken_ms) if self.result_time_taken_ms is not None else '',
            'result_avg_latency_ms': str(self.result_avg_latency_ms) if self.result_avg_latency_ms is not None else '',
            'result_lag_ms': str(self.result_lag_ms) if self.result_lag_ms is not None else '',
            'result_glassflow_rps': str(self.result_glassflow_rps) if self.result_glassflow_rps is not None else ''
        }

    @classmethod
    def from_load_test_config(cls, test_id: str, variant_id: str, load_test_config: Dict) -> 'TestResultModel':
        """Initialize a TestResultModel with parameter fields from load test config"""
        return cls(
            test_id=test_id,
            variant_id=variant_id,
            duration_sec=0.0,  # Default to 0 until test completes
            param_num_processes=load_test_config["num_processes"],
            param_records_per_second=load_test_config["records_per_second"],
            param_total_records=load_test_config["total_records"],
            param_duplication_rate=load_test_config["duplication_rate"],
            param_deduplication_window=load_test_config["deduplication_window"],
            param_max_batch_size=load_test_config["max_batch_size"],
            param_max_delay_time=load_test_config["max_delay_time"]
        )


class TestResultsHandler:
    """Class for handling CSV operations for test results"""
    
    def __init__(self, results_file: Path):  
        print(f"Results file: {results_file}")
        self.results_file = Path(results_file)
        self._ensure_directory()

    def _ensure_directory(self):
        """Ensure the directory for the CSV file exists"""
        self.results_file.parent.mkdir(parents=True, exist_ok=True)

    def write_result(self, result: TestResultModel):
        """Write a single test result to the CSV file"""
        file_exists = self.results_file.exists()
        print(f"Writing result to {self.results_file}")
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=TestResultModel.__fields__.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(result.to_csv_row())

    def get_completed_tests(self) -> List[Dict]:
        """Read completed tests from the CSV file"""
        completed_tests = []
        if self.results_file.exists():
            with open(self.results_file, 'r') as f:
                reader = csv.DictReader(f)
                completed_tests = list(reader)
        return completed_tests

    def read_validated_results(self) -> List[Dict]:
        """
        Reads the CSV file, validates each row using TestResultModel,
        and returns a list of validated JSON objects.
        
        Returns:
            List[Dict]: List of validated test results as JSON objects
            
        Raises:
            FileNotFoundError: If the results file doesn't exist
            ValueError: If any row fails validation
        """
        if not self.results_file.exists():
            raise FileNotFoundError(f"Results file not found: {self.results_file}")

        with open(self.results_file, 'r') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        expected_fields = set(TestResultModel.model_fields.keys())
        parsed_rows = []
        for i, row in enumerate(data):
            try:
                actual_fields = set(row.keys())
                # Step 1: Strict field match check
                if actual_fields != expected_fields:
                    print(f"Row {i} skipped due to mismatched fields: {actual_fields ^ expected_fields}")
                    continue

                # Pydantic handles type conversion
                parsed = TestResultModel(**row)
                parsed_rows.append(parsed.model_dump())
            except Exception as e:
                print(f"Failed to parse row {row}: {e}")
        return parsed_rows

    def display_results(self, test_result: TestResultModel):
        """Display test results in a formatted table"""
        console = Console(width=140)
        table = Table(title="Test Results", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Status", "✅ Success" if test_result.result_success else "❌ Failed")
        table.add_row("Duration", f"{round(test_result.duration_sec, 2)} seconds")
        table.add_row("Records Processed", str(test_result.result_num_records))
        table.add_row("Source RPS in Kafka", str(test_result.result_rps_achieved))
        table.add_row("Average Latency", f"{round(test_result.result_avg_latency_ms, 4)} ms")
        table.add_row("Lag", f"{round(test_result.result_lag_ms, 2)} ms")            
        table.add_row("GlassFlow RPS", f"{round(test_result.result_glassflow_rps, 2)} records/s")
        console.print(table)