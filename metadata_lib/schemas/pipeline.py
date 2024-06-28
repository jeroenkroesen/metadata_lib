from pydantic import BaseModel
from datetime import datetime
from . namespace import Namespace
from typing import Any

class Pipeline(BaseModel):
    id: int
    unid: str | None = None
    namespace: int | str | Namespace
    name: str
    description: str | None = None
    enabled: bool
    version: int
    scope: str
    type: str
    velocity: str
    input_output: dict[Any,Any] | list[Any]
    config: dict[Any,Any] | None = None
    created: datetime
    modified: datetime