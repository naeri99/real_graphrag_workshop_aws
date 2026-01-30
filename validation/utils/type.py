from pydantic import BaseModel
from typing import Dict, Any


class Document(BaseModel):
    page_content: str
    metadata: Dict[str, Any] = {}
