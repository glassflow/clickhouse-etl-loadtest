import base64
from clickhouse_driver import Client
from glassflow_clickhouse_etl import models
from src.utils.logger import log

def create_clickhouse_client(sink_config: models.SinkConfig):
    """Create a ClickHouse client"""
    # GlassFlow uses Clickhouse native port while the python client uses http
    host = sink_config.host
    if sink_config.provider == "localhost":
        host = "localhost"
    
    return Client(
        host=host,
        port=sink_config.port,
        user=sink_config.username,
        password=base64.b64decode(sink_config.password).decode("utf-8"),
        database=sink_config.database,
        secure=sink_config.secure,        
    )

def create_table_if_not_exists(
    sink_config: models.SinkConfig, client, join_key: str = None
):
    """Create a table in ClickHouse if it doesn't exist"""
    if client.execute(f"EXISTS TABLE {sink_config.table}")[0][0]:
        log(
            message=f"Sink [italic u]{sink_config.table}[/italic u]",
            status="Already exists",
            is_success=True,
            component="Clickhouse",
        )
        return
    order_by_column = (
        sink_config.table_mapping[0].column_name if not join_key else join_key
    )
    columns_def = [
        f"{m.column_name} {m.column_type}" for m in sink_config.table_mapping
    ]
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {sink_config.table} ({",".join(columns_def)})
        ENGINE = MergeTree
        ORDER BY {order_by_column};
        """
    )
    log(
        message=f"Sink [italic u]{sink_config.table}[/italic u]",
        status="Created",
        is_success=True,
        component="Clickhouse",
    )

def read_clickhouse_table_size(sink_config: models.SinkConfig, client) -> int:
    """Read the size of a table in ClickHouse"""
    return client.execute(f"SELECT count() FROM {sink_config.table}")[0][0]

def truncate_table(sink_config: models.SinkConfig, client):
    """Truncate a table in ClickHouse"""
    client.execute(f"TRUNCATE TABLE {sink_config.table}")

def get_clickhouse_table_rows(sink_config: models.SinkConfig, client, n_rows: int = 1):
    """Get rows from a ClickHouse table"""
    full_table_name = f"{sink_config.database}.{sink_config.table}"
    query = f"SELECT * FROM {full_table_name} DESC LIMIT {n_rows}"
    result = client.execute(query, with_column_types=True)
    if result[0]:  # result[0] contains rows, result[1] contains column types
        # Get column names from column types
        columns = [col[0] for col in result[1]]
        # Convert row to dictionary
        return [dict(zip(columns, row)) for row in result[0]]
    return [] 


def cleanup_clickhouse(sink_config: models.SinkConfig):
    """Delete all ClickHouse tables that begin with 'load_'"""
    try:
        # Create ClickHouse client with the same configuration as used in the project
        client = create_clickhouse_client(sink_config)

        # Get all tables in the default database
        result = client.execute("SHOW TABLES")
        tables = [row[0] for row in result]  # Extract table names from result rows        
        # Filter tables that begin with 'load_'
        load_tables = [table for table in tables if table.startswith('load_')]
        
        if load_tables:
            # Drop each table
            for table in load_tables:
                client.execute(f"DROP TABLE IF EXISTS {table}")
                log(
                    message=f"Cleanup: Deleted Table [italic u]{table}[/italic u]",
                    status="Deleted",
                    is_success=True,
                    component="Clickhouse",
                )
        else:
            log(
                message="No tables to delete",
                status="Skipped",
                is_success=True,
                component="Clickhouse",
            )
            
    except Exception as e:
        log(
            message="Error cleaning up ClickHouse tables",
            status=str(e),
            is_failure=True,
            component="Clickhouse",
        )
    finally:
        if 'client' in locals():
            client.disconnect()