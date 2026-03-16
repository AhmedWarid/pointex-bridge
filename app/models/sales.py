from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class SaleItem(BaseModel):
    posArticleId: str
    barcode: Optional[str] = None
    articleName: str
    category: Optional[str] = None
    quantitySold: Optional[float] = None
    weightSoldKg: Optional[float] = None
    totalRevenue: float
    unitPrice: float
    transactionCount: int


class SalesMetadata(BaseModel):
    periodFrom: str
    periodTo: str
    totalTransactions: int
    totalRevenue: float
    generatedAt: str
    source: str = "unknown"


class SalesResponse(BaseModel):
    sales: list[SaleItem]
    metadata: SalesMetadata
