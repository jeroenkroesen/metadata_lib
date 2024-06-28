from pydantic import BaseModel
from datetime import datetime
from typing import Any

class Activity(BaseModel):
    id: int
    pipeline: int
    start_time: datetime
    stop_time: datetime
    success: bool
    errors: list[Any]
    input_output: list[Any] | dict[Any,Any]
    rows_read: int
    rows_written: int
    bytes_read: int
    bytes_written: int
    info: list[Any] | dict[Any,Any]