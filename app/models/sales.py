from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class SaleItem(BaseModel):
    posArticleId: str
    barcode: Optional[str] = None
    articleName: str
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


class SalesResponse(BaseModel):
    sales: list[SaleItem]
    metadata: SalesMetadata
