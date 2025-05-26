import uuid
import json
import time
from typing import Dict, List
from src.pipeline_test import run_variant
from src.utils.pipeline import GlassFlowPipeline
from src.utils.clickhouse import cleanup_clickhouse
from src.utils.kafka import cleanup_kafka
from src.utils.metrics import TestResultModel, TestResultsHandler
from rich.console import Console
from rich.panel import Panel
import os
console = Console(width=140)

class TestExecutor:
    def __init__(self, results_dir: str, 
                 test_id: str, 
                 pipeline_config_path: str, 
                 glassflow_host: str = "http://localhost:8080", 
                 event_schema: str = "config/glassgen/user_event.json"):
        self.test_id = test_id        
        self.pipeline_config_path = pipeline_config_path
        self.glassflow_host = glassflow_host
        self.event_schema = event_schema
        results_file = os.path.join(results_dir, f"{test_id}_results.csv")
        self.result_writer = TestResultsHandler(results_file)
    
    def _create_variant_id(self, config: Dict) -> str:
        """Create a unique test ID for a configuration"""
        # Create a deterministic test ID based on configuration
        config_str = json.dumps(config, sort_keys=True)
        config_hash = str(uuid.uuid5(uuid.NAMESPACE_DNS, config_str))[:8]
        return f"load_{config_hash}" 

    def run_variant_test(self, variant_id: str, load_test_config: Dict):
        """Run a single test configuration"""
        pipeline = GlassFlowPipeline(host=self.glassflow_host)
        pipeline_config = pipeline.load_conf(json.load(open(self.pipeline_config_path)))
        cleanup_kafka(pipeline_config.source)
        cleanup_clickhouse(pipeline_config.sink)

        start_time = time.time()
        test_result = TestResultModel.from_load_test_config(self.test_id, variant_id, load_test_config)        
        try:            
            test_result = run_variant(self.pipeline_config_path, self.event_schema, variant_id, load_test_config, pipeline, test_result)
            duration = time.time() - start_time
            test_result.duration_sec = duration        
            cleanup_kafka(pipeline_config.source)
            cleanup_clickhouse(pipeline_config.sink)
            pipeline.cleanup_pipeline()          
            print(f"Test result: {test_result.result_success}")
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
            test_result.result_success = False
            test_result.duration_sec = duration

        # now write the test result to the file 
        self.result_writer.write_result(test_result)
        self.result_writer.display_results(test_result)

    def run_tests(self, resume: bool = True, variant_configs: List[Dict] = None):
        """Run all test configurations, with option to resume from last completed test"""
        # Get test configurations        
        # Get completed tests if resuming
        completed_tests = self.result_writer.get_completed_tests() if resume else []
        completed_variant_ids = {test["variant_id"] for test in completed_tests}
        
        # Print test execution header
        console.print(Panel(
            f"[bold blue]Test ID:[/bold blue] {self.test_id}\n"
            f"[bold blue]Total Configurations:[/bold blue] {len(variant_configs)}\n"
            f"[bold blue]Resume Mode:[/bold blue] {'Enabled' if resume else 'Disabled'}",
            title="🚀 Test Execution Started",
            border_style="blue"
        ))
        
        # Run each test configuration
        for i, config in enumerate(variant_configs, 1):
            variant_id = self._create_variant_id(config)
            if resume and variant_id in completed_variant_ids:                
                console.print(Panel(
                    f"[bold cyan]Test {i}/{len(variant_configs)}[/bold cyan]\n"
                    f"[bold cyan]Variant ID:[/bold cyan] {variant_id}\n\n"
                    f"[bold cyan]Configuration:[/bold cyan]\n{json.dumps(config, indent=2)}",
                    title="⏭️ Skipped CompletedTest",
                    border_style="cyan"
                ))
                continue

            # Print test configuration
            console.print(Panel(
                f"[bold cyan]Test {i}/{len(variant_configs)}[/bold cyan]\n"
                f"[bold cyan]Variant ID:[/bold cyan] {variant_id}\n\n"
                f"[bold cyan]Configuration:[/bold cyan]\n{json.dumps(config, indent=2)}",
                title="🔄 Running Test",
                border_style="cyan"
            ))
            
            self.run_variant_test(variant_id, config)
