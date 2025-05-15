import os
import csv
import json
import time
from datetime import datetime
from typing import Dict, List
import uuid
from load_test_generator import LoadTestGenerator
from pippeline_test import run_variant
from cleanup import cleanup_kafka, cleanup_clickhouse, cleanup_pipeline
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from utils import log

console = Console(width=140)

class TestExecutor:
    def __init__(self, config_path: str, results_dir: str, test_id: str):
        self.generator = LoadTestGenerator(config_path)
        self.test_id = test_id
        self.results_dir = results_dir
        self.pipeline_config_path = "config/glassflow/deduplication_pipeline.json"
        self.generator_schema = "config/glassgen/user_event.json"
        self.results_file = os.path.join(results_dir, f"{self.test_id}_results.csv")
        self._ensure_results_dir()
        
    def _ensure_results_dir(self):
        """Create results directory if it doesn't exist"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

    def _get_completed_tests(self) -> List[Dict]:
        """Read completed tests from results file"""
        completed_tests = []
        if os.path.exists(self.results_file):
            with open(self.results_file, 'r') as f:
                reader = csv.DictReader(f)
                completed_tests = list(reader)
        return completed_tests

    def _save_test_result(self, result: Dict):
        """Save a single test result to CSV"""
        file_exists = os.path.exists(self.results_file)
        
        with open(self.results_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)

    def _create_variant_id(self, config: Dict) -> str:
        """Create a unique test ID for a configuration"""
        # Create a deterministic test ID based on configuration
        config_str = json.dumps(config, sort_keys=True)
        config_hash = str(uuid.uuid5(uuid.NAMESPACE_DNS, config_str))[:8]
        return f"load_{config_hash}" 

    def _prepare_test_result(self, variant_id: str, config: Dict, success: bool, 
                        duration: float, run_metrics: Dict) -> Dict:
        """Prepare a test result dictionary"""
        return {
            "test_id": self.test_id,
            "variant_id": variant_id,
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "duration_sec": round(duration, 2),
            **{f"param_{k}": v for k, v in config.items()},
            **{f"result_{k}": v for k, v in run_metrics.items()}
        }

    def run_variant_test(self, variant_id: str, config: Dict):
        """Run a single test configuration"""
        cleanup_kafka()
        cleanup_clickhouse()
        # Skip if test was already completed        
        start_time = time.time()
        run_metrics = {}
        try:
            # Set up pipeline with test configuration
            run_metrics = run_variant(self.pipeline_config_path, self.generator_schema, variant_id, config)
            duration = time.time() - start_time
            result = self._prepare_test_result(
                variant_id=variant_id,
                config=config,
                success=run_metrics["success"],
                duration=duration, 
                run_metrics=run_metrics
            )
            self._save_test_result(result)

            # Create a table for test results
            table = Table(title="Test Results", show_header=True, header_style="bold magenta")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")

            table.add_row("Status", "✅ Success" if run_metrics["success"] else "❌ Failed")
            table.add_row("Duration", f"{round(duration, 2)} seconds")
            table.add_row("Records Processed", str(run_metrics.get('num_records', 0)))
            table.add_row("RPS Achieved", str(run_metrics.get('rps_achieved', 0)))
            table.add_row("Average Latency", f"{round(run_metrics.get('avg_latency_ms', 0), 2)} ms")

            console.print(table)
            
            # cleanup kafka and clickhouse
            cleanup_kafka()
            cleanup_clickhouse()
            # cleanup pipeline
            cleanup_pipeline()
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            
            # Create error panel
            error_panel = Panel(
                f"[red]Test failed with error:[/red]\n{error_msg}\n\nDuration: {round(duration, 2)} seconds",
                title="❌ Test Failure",
                border_style="red"
            )
            console.print(error_panel)
                        
            # Save failed test result
            result = self._prepare_test_result(
                variant_id=variant_id,
                config=config,
                success=False,
                duration=duration,
                run_metrics=run_metrics
            )
            self._save_test_result(result)

    def run_tests(self, resume: bool = True):
        """Run all test configurations, with option to resume from last completed test"""
        # Get all test configurations
        all_configs = self.generator.generate_combinations()
        
        # Get completed tests if resuming
        completed_tests = self._get_completed_tests() if resume else []
        completed_variant_ids = {test["variant_id"] for test in completed_tests}
        
        # Print test execution header
        console.print(Panel(
            f"[bold blue]Test ID:[/bold blue] {self.test_id}\n"
            f"[bold blue]Total Configurations:[/bold blue] {len(all_configs)}\n"
            f"[bold blue]Resume Mode:[/bold blue] {'Enabled' if resume else 'Disabled'}",
            title="🚀 Test Execution Started",
            border_style="blue"
        ))
        
        # Run each test configuration
        for i, config in enumerate(all_configs, 1):
            variant_id = self._create_variant_id(config)
            if resume and variant_id in completed_variant_ids:                
                console.print(Panel(
                    f"[bold cyan]Test {i}/{len(all_configs)}[/bold cyan]\n"
                    f"[bold cyan]Variant ID:[/bold cyan] {variant_id}\n\n"
                    f"[bold cyan]Configuration:[/bold cyan]\n{json.dumps(config, indent=2)}",
                    title="⏭️ Skipped CompletedTest",
                    border_style="cyan"
                ))
                continue

            # Print test configuration
            console.print(Panel(
                f"[bold cyan]Test {i}/{len(all_configs)}[/bold cyan]\n"
                f"[bold cyan]Variant ID:[/bold cyan] {variant_id}\n\n"
                f"[bold cyan]Configuration:[/bold cyan]\n{json.dumps(config, indent=2)}",
                title="🔄 Running Test",
                border_style="cyan"
            ))
            
            self.run_variant_test(variant_id, config)

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Run load tests with configurable parameters')
    # add a test-id argument and it is required
    parser.add_argument('--test-id', required=True,
                       help='Main test ID')
    parser.add_argument('--no-resume', action='store_true', 
                       help='Do not resume from previous test run')
    parser.add_argument('--results-dir', default='results',
                       help='Directory to store test results')
    parser.add_argument('--config', default='config/load_test_params.json',
                       help='Path to load test parameters configuration file (default: config/load_test_params.json)')
    args = parser.parse_args()

    executor = TestExecutor(
        config_path=args.config,
        results_dir=args.results_dir,
        test_id=args.test_id        
    )
    executor.run_tests(resume=not args.no_resume)

if __name__ == "__main__":
    main()
