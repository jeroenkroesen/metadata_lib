from pydantic import BaseModel
from datetime import datetime
from . namespace import Namespace

class Watermark(BaseModel):
    id: int
    pipeline: int | str
    input_entity: int | str
    high_watermark: datetime
    created: datetime
    modified: datetime