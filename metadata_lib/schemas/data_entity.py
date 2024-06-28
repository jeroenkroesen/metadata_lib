from pydantic import BaseModel
from datetime import datetime
from . namespace import Namespace
from . system import System
from . schema import Schema
from typing import Any

class Data_entity(BaseModel):
    id: int
    unid: str | None = None
    namespace: int | str | Namespace
    system: int | str | System
    name: str
    description: str | None = None
    type: str
    interface: str
    entity_schema: int | str | Schema
    checks: list[Any] | None = None
    config: dict[Any,Any] | None = None
    created: datetime
    modified: datetime