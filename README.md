# GlassFlow Clickhouse ETL LoadTesting

A load testing tool for [GlassFlow Clickhouse ETL](https://github.com/glassflow/clickhouse-etl), designed to evaluate the performance and reliability of real-time data processing pipelines.

## Overview

This tool performs load testing on GlassFlow Clickhouse ETL by:
- Generating synthetic data using [glassgen](https://github.com/glassflow/glassgen)
- Sending data to Kafka topics
- Measuring the performance of data processing through the ETL pipeline
- Collecting and analyzing metrics

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Local machine with sufficient resources to run the test

## Installation

1. Clone this repository:
```bash
git clone https://github.com/your-username/clickhouse-etl-loadtest.git
cd clickhouse-etl-loadtest
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The load test parameters are defined in `config/load_test_params.json`. Here are the available parameters:

### Test Parameters

| Parameter | Description | Range/Values |
|-----------|-------------|--------------|
| num_processes | Number of parallel processes | 4-12 (step: 2) |
| records_per_second | Records per second to generate | 10,000-50,000 (step: 10,000) |
| total_records | Total number of records to generate | 500,000-5,000,000 (step: 500,000) |
| duplication_rate | Rate of duplicate records | 0.1 (10% duplicates) |
| deduplication_window | Time window for deduplication | ["1h", "4h"] |
| max_batch_size | Max batch size for the sink | [5000] |
| max_delay_time | Max delay time for the sink | ["10s"] |

### Configuring Test Parameters

You can customize the test parameters by editing `config/load_test_params.json`. For each parameter, you can set:
- `min`: Minimum value
- `max`: Maximum value
- `step`: Increment between values
- `values`: Fixed list of values (if step is not used)

Example configuration:
```json
{
    "num_processes": {
        "min": 4,
        "max": 12,
        "step": 2
    },
    "records_per_second": {
        "min": 10000,
        "max": 50000,
        "step": 10000
    }
}
```

To limit the number of test variants, you can set `max_combinations` in the configuration file. This is useful when you want to test a subset of all possible combinations.

## Running the Tests

1. Configure your test parameters:
   - Edit `config/load_test_params.json` to set your desired parameter ranges
   - Optionally set `max_combinations` to limit the number of test variants
   - Save the configuration file

2. Start the required services:
```bash
docker-compose up -d
```

3. Run the load test:
```bash
python main.py --test-id <your-test-id>
```

Additional options:
- `--no-resume`: Do not resume from previous test run
- `--results-dir`: Directory to store test results (default: 'results')
- `--config`: Path to load test parameters configuration file (default: 'config/load_test_params.json')

Example with custom configuration:
```bash
python main.py --test-id my-test --config path/to/your/load_test_params.json
```

## Test Results

The test results are stored in the `results` directory (or specified directory) with the following format:
- CSV file: `<test-id>_results.csv`

### Metrics Collected

The following metrics are collected and analyzed for each test run:

| Metric | Description | Unit |
|--------|-------------|------|
| duration_sec | Total time taken for the test | seconds |
| result_num_records | Number of records processed | count |
| result_time_taken_publish_ms | Time taken to publish records to Kafka | milliseconds |
| result_time_taken_ms | Time taken to process records through the pipeline | milliseconds |
| result_rps_achieved | Records per second achieved during the test | records/second |
| result_avg_latency_ms | Average latency per record | milliseconds |
| result_success | Whether the test completed successfully | boolean |

These metrics provide insights into:
- Overall test performance (duration, success rate)
- Data processing throughput (RPS)
- Processing efficiency (latency)
- System reliability (success rate)

## Analyzing Results

The tool includes a results analysis script (`results.py`) that helps you analyze and visualize the test results. To analyze your test results:

Run the analysis script with the path to your results file:
```bash
python results.py --results-file results/<your-test-id>_results.csv
```

The script will display:
- A list of all available columns in the results file
- A formatted table showing the important metrics for each test variant:
  - Variant ID
  - Time taken for the test
  - Number of records processed
  - Time taken to publish
  - RPS achieved
  - Time taken to process records
  - Average latency
  - Success status

The results are presented in a clear, tabular format with color-coded headers and values for better readability.

Error handling:
- If the results file is not found, an error message will be displayed in red
- Any other errors during analysis will be clearly reported

## Architecture

The load test runs on your local machine and interacts with:
- Kafka: Running in Docker for message streaming
- ClickHouse: Running in Docker for data storage
- GlassFlow ETL: Running in Docker for data processing

## Cleanup

The tool includes cleanup utilities to remove test artifacts:
- `cleanup_kafka()`: Removes Kafka topics starting with 'load_'
- `cleanup_clickhouse()`: Removes ClickHouse tables starting with 'load_'


## TODO 

- Cleanup console printing during the test 
- Add more information about resource consumption during the test
