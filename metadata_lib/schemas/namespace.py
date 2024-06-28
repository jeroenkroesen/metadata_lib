from pydantic import BaseModel
from datetime import datetime

class Namespace(BaseModel):
    id: int
    unid: str | None = None
    name: str
    description: str | None = None
    created: datetime
    modified: datetime