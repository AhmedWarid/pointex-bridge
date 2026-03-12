from typing import Optional

from pydantic import BaseModel


class Article(BaseModel):
    posArticleId: str
    barcode: Optional[str] = None
    name: str
    category: Optional[str] = None  # V2: resolve from lookup table
    sellingPrice: Optional[float] = None  # V2: resolve from TARIFS table
    costPrice: Optional[float] = None
    unit: str = "piece"  # V2: resolve from UNITES.DB
    isActive: bool = True
    updatedAt: Optional[str] = None


class ArticlesResponse(BaseModel):
    articles: list[Article]
    totalCount: int
