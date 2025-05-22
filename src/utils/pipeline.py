from __future__ import annotations

import time
from glassflow_clickhouse_etl import errors, models, Pipeline
from rich.console import Console
from src.utils.logger import log

console = Console(width=140)

class GlassFlowPipeline:
    """Class to handle all GlassFlow pipeline operations"""
    
    def __init__(self, host: str = "http://localhost:8080"):
        """Initialize GlassFlowPipeline with host configuration
        
        Args:
            host (str): GlassFlow host URL. Defaults to "http://localhost:8080"
        """
        self.host = host
        self.pipeline = None

    @staticmethod
    def load_conf(config_json: dict) -> models.PipelineConfig:
        """Load pipeline configuration from a JSON file"""
        return models.PipelineConfig(**config_json)

    def stop_pipeline_if_running(self):
        """Stop a pipeline if it is running"""
        try:
            pipeline_id = Pipeline(url=self.host).get_running_pipeline()
        except errors.PipelineNotFoundError:
            return    
        self.delete_pipeline()

    def check_if_pipeline_exists(self, config: models.PipelineConfig) -> tuple[bool, str | None]:
        """
        Check if a pipeline exists

        Returns:
            bool: True if the pipeline exists, False otherwise
            str | None: Pipeline ID of existing pipeline, None if it doesn't exist
        """
        try:
            pipeline_id = Pipeline(url=self.host).get_running_pipeline()
            if pipeline_id == config.pipeline_id:
                return True, pipeline_id
            else:
                return False, pipeline_id
        except errors.ConnectionError:
            log(
                message="Looks like [bold orange3]GlassFlow[/bold orange3] is not running locally!",
                status="",
                is_failure=True,
                component="GlassFlow",
            )
            print("\nRun the following command to start it:\n  > `docker compose up -d`\n")
            exit(1)
        except errors.PipelineNotFoundError:
            return False, None
        except Exception as e:
            log(
                message="Error checking if pipeline exists",
                status=str(e),
                is_failure=True,
                component="GlassFlow",
            )
            raise e

    def delete_pipeline(self):
        """Delete a pipeline"""
        Pipeline(url=self.host).delete()

    def create_pipeline(self, config: models.PipelineConfig) -> Pipeline:
        """
        Create GlassFlow pipeline

        Args:
            config (models.PipelineConfig): Pipeline configuration

        Returns:
            Pipeline: GlassFlow pipeline
        """
        self.pipeline = Pipeline(config, url=self.host)
        try:
            self.pipeline.create()
            with console.status(
                "[bold green]Waiting for pipeline to start...[/bold green]",
                spinner="dots",
            ):
                time.sleep(10)
            log(
                message=f"Pipeline [italic u]{config.pipeline_id}[/italic u]",
                status="Created",
                is_success=True,
                component="GlassFlow",
            )
        except errors.PipelineAlreadyExistsError:
            log(
                message=f"Pipeline [italic u]{config.pipeline_id}[/italic u]",
                status="Already exists",
                is_failure=True,
                component="GlassFlow",
            )
        except Exception as e:
            log(
                message=f"Error creating pipeline [italic u]{config.pipeline_id}[/italic u]",
                status=str(e),
                is_failure=True,
                component="GlassFlow",
            )
            raise e

        return self.pipeline

    def cleanup_pipeline(self):
        """Clean up the pipeline by stopping it if running"""
        self.stop_pipeline_if_running()
        log(
            message="Cleanup: Stopped Pipeline",
            status="Deleted",
            is_success=True,
            component="Pipeline",
        )
