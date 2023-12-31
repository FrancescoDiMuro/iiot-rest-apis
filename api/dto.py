from pydantic import BaseModel

class Tags(BaseModel):

    id: int | None = None
    name: str
    description: str
    address: str
    collection_interval: str
    low_limit: float
    high_limit: float
    egu: str
    created_at: str | None = None
    updated_at: str | None = None
    deleted_at: str | None = None


class Data(BaseModel):

    name: str    
    timestamp: str
    value: float
    