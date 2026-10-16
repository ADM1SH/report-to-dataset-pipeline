from typing import List
from pydantic import BaseModel

class ETLManifest(BaseModel):
    pdf_sha256: str
    release_edition: str
    extracted_at_iso: str
    tables_extracted: int
    total_rows: int
    table_names: List[str]
