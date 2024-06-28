from pydantic import BaseModel
from datetime import datetime
from . namespace import Namespace
from typing import Any

class Schema(BaseModel):
    id: int
    unid: str | None = None
    namespace: int | str | Namespace
    name: str
    description: str | None = None
    type: str
    version: int
    entity_schema: list[Any] | dict[Any,Any] | None = None
    created: datetime
    modified: datetime