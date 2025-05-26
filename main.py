from src.test_executor import TestExecutor
import json
from rich.console import Console
from rich.panel import Panel
from src.models import LoadTestConfig, SingleTestConfig
from src.load_test_generator import LoadTestGenerator


console = Console(width=140)

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
    parser.add_argument('--config', default='load_test_params.json',
                       help='Path to load test parameters configuration file (default: load_test_params.json)')
    parser.add_argument('--single-config', type=str,
                       help='JSON file of a single test configuration to run')
    parser.add_argument('--pipeline-config', type=str,
                       help='JSON file of a pipeline configuration to run', default="config/glassflow/deduplication_pipeline.json")
    parser.add_argument('--glassflow-host', type=str, default='http://localhost:8080',
                       help='GlassFlow host URL (default: http://localhost:8080)')
    
    args = parser.parse_args()    
    executor = TestExecutor(    
        results_dir=args.results_dir,
        test_id=args.test_id,
        pipeline_config_path=args.pipeline_config,
        glassflow_host=args.glassflow_host
    )

    single_config = None  
    combinations = []  
    if args.single_config:
        try:
            with open(args.single_config) as f:
                single_config = SingleTestConfig.model_validate_json(f.read())                
                combinations.append(single_config.model_dump())
        except json.JSONDecodeError:
            console.print(Panel(
                "[red]Invalid JSON format for --single-config parameter[/red]",
                title="❌ Error",
                border_style="red"
            ))
            return
        except Exception as e:
            console.print(Panel(
                f"[red]Invalid configuration format: {str(e)}[/red]",
                title="❌ Error",
                border_style="red"
            ))
            return
    else:
        # generate combinations from the config file
        try:            
            generator = LoadTestGenerator(args.config)
            combinations = generator.generate_combinations()
        except Exception as e:
            console.print(Panel(
                f"[red]Invalid configuration format: {str(e)}[/red]",
                title="❌ Error",
                border_style="red"
            ))
            return

    # run the tests
    executor.run_tests(resume=not args.no_resume, variant_configs=combinations)

if __name__ == "__main__":
    main()
