# src/api/schemas.py

from pydantic import BaseModel
from typing import Optional, List

# ============ PROPERTY SCHEMAS ============

class PropertyBase(BaseModel):
    # Vì file fact_properties.csv không có cột title, mình để Optional để tránh lỗi
    title: Optional[str] = "Bất động sản" 
    price_million: float  # SỬA: Khớp với models.py và CSV
    area: float
    bedrooms_num: int     # SỬA: Khớp với models.py và CSV
    bathrooms_num: int
    location_id: int
    type_id: int

class PropertyCreate(PropertyBase):
    pass

class PropertyResponse(PropertyBase):
    property_id: int
    price_per_m2: float
    city: str
    district: str
    
    class Config:
        from_attributes = True

# ============ PRICE PREDICTION SCHEMAS ============

class PricePredictionRequest(BaseModel):
    area: float
    bedrooms_num: int     # SỬA: Khớp với models.py
    bathrooms_num: int
    city: str
    property_type: str

class PricePredictionResponse(BaseModel):
    predicted_price: float
    confidence: float
    price_range_min: float
    price_range_max: float
    market_average: float

# ============ RECOMMENDATION SCHEMAS ============

class RecommendationRequest(BaseModel):
    property_id: int
    limit: int = 5

class RecommendationResponse(BaseModel):
    similar_properties: List[PropertyResponse]
    similarity_score: List[float]

# ============ STATISTICS SCHEMAS ============

class CityStatistics(BaseModel):
    city: str
    total_properties: int
    average_price: float
    average_area: float
    average_bedrooms: float
    min_price: float
    max_price: float

class MarketStatistics(BaseModel):
    total_properties: int
    total_cities: int
    average_price: float
    average_area: float
    cities_stats: List[CityStatistics]