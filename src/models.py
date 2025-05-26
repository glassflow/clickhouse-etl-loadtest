from typing import List, Union
from pydantic import BaseModel, Field

class ParameterRange(BaseModel):
    min: Union[int, float]
    max: Union[int, float]
    step: Union[int, float]
    description: str

class ParameterValues(BaseModel):
    values: List[Union[str, int]]
    description: str

class LoadTestParameters(BaseModel):
    num_processes: ParameterRange = Field(
        default=ParameterRange(
            min=1,
            max=1,
            step=1,
            description="Number of parallel processes"
        )
    )
    num_processes: ParameterRange = 1    
    total_records: ParameterRange
    duplication_rate: ParameterRange = Field(
        default=ParameterRange(
            min=0.1,
            max=0.1,
            step=0.1,
            description="Rate of duplicate records (0.1 = 10% duplicates)"
        )
    )
    deduplication_window: ParameterValues = Field(
        default=ParameterValues(
            values=["8h"],
            description="Time window for deduplication"
        )
    )
    max_batch_size: ParameterValues = Field(
        default=ParameterValues(
            values=[5000],
            description="Max batch size for the sink"
        )
    )
    max_delay_time: ParameterValues = Field(
        default=ParameterValues(
            values=["10s"],
            description="Max delay time for the sink"
        )
    )

class SingleTestConfig(BaseModel):
    num_processes: int = 1    
    total_records: int
    duplication_rate: float = 0.1
    deduplication_window: str = "8h"
    max_batch_size: int = 5000
    max_delay_time: str = "10s"

class LoadTestConfig(BaseModel):
    parameters: LoadTestParameters
    max_combinations: int = 1
