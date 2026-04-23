# src/api/main.py

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
import yaml
import sys
from typing import List

sys.path.append('..')
from src.api.schemas import (
    PropertyResponse, PricePredictionRequest, PricePredictionResponse,
    RecommendationRequest, RecommendationResponse, MarketStatistics, CityStatistics
)

# THÊM DÒNG NÀY ĐỂ FIX LỖI IMPORT:
from src.api.models import Base, FactProperties, DimLocation
from src.utils.logger_config import setup_logger

logger = setup_logger()

# ============ LOAD CONFIG ============
with open('config/config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# ============ DATABASE SETUP ============
db_config = config['database']
DATABASE_URL = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ============ FASTAPI SETUP ============
app = FastAPI(title="Real Estate API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ============ ENDPOINTS ============

@app.get("/api/properties", response_model=List[PropertyResponse])
def get_properties(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    city: str = Query(None),
    db: Session = Depends(get_db)
):
    logger.info(f"📍 Getting properties: skip={skip}, limit={limit}, city={city}")
    
    # Phải Join với DimLocation để có dữ liệu city/district
    query = db.query(FactProperties).join(DimLocation)
    
    if city:
        query = query.filter(DimLocation.city == city)
    
    properties = query.offset(skip).limit(limit).all()
    
    # Bước này cực kỳ quan trọng để không bị lỗi 500:
    for p in properties:
        # Lấy dữ liệu từ bảng DimLocation thông qua relationship
        # Đảm bảo trong models.py bạn đã có: location = relationship("DimLocation")
        p.city = p.location.city
        p.district = p.location.district
        p.title = f"Bất động sản tại {p.district}"
        
        # FastAPI sẽ tự động map p.price_million và p.bedrooms_num 
        # vào PropertyResponse nếu tên ở Schema và Model khớp nhau.
    
    logger.info(f"✅ Found {len(properties)} properties")
    return properties

@app.post("/api/predict", response_model=PricePredictionResponse)
def predict_price(request: PricePredictionRequest, db: Session = Depends(get_db)):
    market_data = db.query(FactProperties).join(DimLocation).filter(
        DimLocation.city == request.city,
        FactProperties.area > request.area * 0.8,
        FactProperties.area < request.area * 1.2
    ).all()
    
    if not market_data:
        avg_price = 5.0
    else:
        # Sử dụng price_million đã sửa khớp với model của bạn
        prices = [p.price_million for p in market_data]
        avg_price = sum(prices) / len(prices)
        
    return PricePredictionResponse(
        predicted_price=round(avg_price, 2),
        confidence=0.85,
        price_range_min=round(avg_price * 0.9, 2),
        price_range_max=round(avg_price * 1.1, 2),
        market_average=round(avg_price, 2)
    )

@app.post("/api/recommendations", response_model=RecommendationResponse)
def get_recommendations(request: RecommendationRequest, db: Session = Depends(get_db)):
    target = db.query(FactProperties).filter(FactProperties.property_id == request.property_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Property not found")
    
    # Dùng bedrooms_num đã sửa khớp với model của bạn
    similar_properties = db.query(FactProperties).join(DimLocation).filter(
        DimLocation.city == target.location.city,
        FactProperties.property_id != target.property_id,
        FactProperties.bedrooms_num == target.bedrooms_num
    ).limit(request.limit).all()
    
    for s in similar_properties:
        s.city = s.location.city
        s.district = s.location.district
        s.title = f"Bất động sản tại {s.district}"

    return RecommendationResponse(
        similar_properties=similar_properties,
        similarity_score=[0.95] * len(similar_properties)
    )

@app.get("/api/statistics", response_model=MarketStatistics)
def get_statistics(db: Session = Depends(get_db)):
    total_properties = db.query(func.count(FactProperties.property_id)).scalar()
    cities_rows = db.query(DimLocation.city).distinct().all()
    cities = [c[0] for c in cities_rows]
    
    avg_price = db.query(func.avg(FactProperties.price_million)).scalar() or 0
    avg_area = db.query(func.avg(FactProperties.area)).scalar() or 0
    
    cities_stats = []
    for city_name in cities:
        city_data = db.query(FactProperties).join(DimLocation).filter(DimLocation.city == city_name).all()
        if city_data:
            prices = [p.price_million for p in city_data]
            cities_stats.append(CityStatistics(
                city=city_name,
                total_properties=len(city_data),
                average_price=round(sum(prices)/len(prices), 2),
                average_area=round(sum([p.area for p in city_data])/len(city_data), 2),
                average_bedrooms=round(sum([p.bedrooms_num for p in city_data])/len(city_data), 2),
                min_price=min(prices),
                max_price=max(prices)
            ))
            
    return MarketStatistics(
        total_properties=total_properties,
        total_cities=len(cities),
        average_price=round(avg_price, 2),
        average_area=round(avg_area, 2),
        cities_stats=cities_stats
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)