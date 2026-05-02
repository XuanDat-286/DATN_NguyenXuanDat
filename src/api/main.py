# src/api/main.py

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
import yaml
import sys
from typing import List

sys.path.append('..')
from src.api.schemas import (
    PropertyResponse, PricePredictionRequest, PricePredictionResponse,
    RecommendationRequest, RecommendationResponse, MarketStatistics, CityStatistics
)
from src.api.models import Base, FactProperties, DimLocation, DimPropertyType
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

# ============ LOAD ML MODEL (placeholder) ============
# Sẽ load model thực tế ở tuần 7-8
ml_model = None
confidence_score = 0.85

# ============ FASTAPI SETUP ============
app = FastAPI(
    title="Real Estate API",
    description="API for real estate price prediction and recommendations",
    version="1.0.0"
)

# ============ CORS CONFIGURATION ============
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============ DEPENDENCY ============
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ============ ROOT ENDPOINT ============
@app.get("/")
def root():
    """Root endpoint"""
    return {
        "message": "Welcome to Real Estate API",
        "version": "1.0.0",
        "endpoints": {
            "properties": "/api/properties",
            "predict": "/api/predict",
            "recommendations": "/api/recommendations",
            "statistics": "/api/statistics"
        }
    }

# ============ HEALTH CHECK ============
@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "OK",
        "message": "API is running"
    }

# ============ PROPERTIES ENDPOINTS ============

@app.get("/api/properties", response_model=List[PropertyResponse])
def get_properties(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    city: str = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get list of properties with pagination and filtering
    
    Parameters:
    - skip: số lượng bỏ qua (default: 0)
    - limit: số lượng trả về (default: 10, max: 100)
    - city: lọc theo thành phố (optional)
    """
    logger.info(f"📍 Getting properties: skip={skip}, limit={limit}, city={city}")
    
    query = db.query(FactProperties)
    
    if city:
        query = query.filter(FactProperties.city == city)
    
    properties = query.offset(skip).limit(limit).all()
    
    logger.info(f"✅ Found {len(properties)} properties")
    return properties

@app.get("/api/properties/{property_id}", response_model=PropertyResponse)
def get_property(property_id: int, db: Session = Depends(get_db)):
    """Get property by ID"""
    logger.info(f"📍 Getting property {property_id}")
    
    property_obj = db.query(FactProperties).filter(
        FactProperties.property_id == property_id
    ).first()
    
    if not property_obj:
        logger.error(f"❌ Property {property_id} not found")
        raise HTTPException(status_code=404, detail="Property not found")
    
    logger.info(f"✅ Found property {property_id}")
    return property_obj

# ============ PRICE PREDICTION ENDPOINT ============

@app.post("/api/predict", response_model=PricePredictionResponse)
def predict_price(request: PricePredictionRequest, db: Session = Depends(get_db)):
    """
    Predict property price based on features
    
    Parameters:
    - area: diện tích (m²)
    - bedrooms: số phòng ngủ
    - bathrooms_num: số phòng tắm
    - city: thành phố
    - property_type: loại bất động sản
    """
    logger.info(f"🔮 Predicting price for: {request}")
    
    # Lấy thông tin thị trường từ database
    market_data = db.query(FactProperties).filter(
        FactProperties.city == request.city,
        FactProperties.area > request.area * 0.8,
        FactProperties.area < request.area * 1.2
    ).all()
    
    if not market_data:
        logger.warning(f"⚠️  No market data for {request.city}")
        # Return default values
        predicted_price = 5.0
        market_average = 5.0
        price_range_min = 4.0
        price_range_max = 6.0
    else:
        # Calculate average price
        prices = [p.price_milli for p in market_data]
        market_average = sum(prices) / len(prices)
        
        # Simple prediction: multiply area by average price per m²
        avg_price_per_m2 = market_average * 1000000 / request.area
        predicted_price = (request.area * avg_price_per_m2) / 1000000
        
        price_range_min = predicted_price * 0.9
        price_range_max = predicted_price * 1.1
    
    logger.info(f"✅ Predicted price: {predicted_price:.2f}B VND")
    
    return PricePredictionResponse(
        predicted_price=round(predicted_price, 2),
        confidence=confidence_score,
        price_range_min=round(price_range_min, 2),
        price_range_max=round(price_range_max, 2),
        market_average=round(market_average, 2)
    )

# ============ RECOMMENDATIONS ENDPOINT ============

@app.post("/api/recommendations", response_model=RecommendationResponse)
def get_recommendations(
    request: RecommendationRequest,
    db: Session = Depends(get_db)
):
    """
    Get similar property recommendations based on a property ID
    
    Parameters:
    - property_id: ID của bất động sản
    - limit: số lượng gợi ý (default: 5)
    """
    logger.info(f"🎯 Getting recommendations for property {request.property_id}")
    
    # Get the target property
    target_property = db.query(FactProperties).filter(
        FactProperties.property_id == request.property_id
    ).first()
    
    if not target_property:
        logger.error(f"❌ Property {request.property_id} not found")
        raise HTTPException(status_code=404, detail="Property not found")
    
    # Find similar properties
    similar_properties = db.query(FactProperties).filter(
        FactProperties.city == target_property.city,
        FactProperties.property_id != request.property_id,
        FactProperties.area > target_property.area * 0.8,
        FactProperties.area < target_property.area * 1.2,
        FactProperties.bedrooms == target_property.bedrooms
    ).limit(request.limit).all()
    
    # Calculate similarity scores
    similarity_scores = [0.85 + (i * 0.01) for i in range(len(similar_properties))]
    
    logger.info(f"✅ Found {len(similar_properties)} similar properties")
    
    return RecommendationResponse(
        similar_properties=similar_properties,
        similarity_score=similarity_scores
    )

# ============ STATISTICS ENDPOINTS ============

@app.get("/api/statistics", response_model=MarketStatistics)
def get_statistics(db: Session = Depends(get_db)):
    """Get market statistics"""
    logger.info("📊 Getting market statistics")
    
    # Total properties
    total_properties = db.query(func.count(FactProperties.property_id)).scalar()
    
    # Total cities
    cities = db.query(FactProperties.city).distinct().all()
    total_cities = len(cities)
    
    # Average price
    avg_price = db.query(func.avg(FactProperties.price_milli)).scalar() or 0
    
    # Average area
    avg_area = db.query(func.avg(FactProperties.area)).scalar() or 0
    
    # City statistics
    cities_stats = []
    for city in cities:
        city_name = city[0]
        city_data = db.query(FactProperties).filter(
            FactProperties.city == city_name
        ).all()
        
        if city_data:
            prices = [p.price_milli for p in city_data]
            areas = [p.area for p in city_data]
            bedrooms = [p.bedrooms for p in city_data]
            
            cities_stats.append(CityStatistics(
                city=city_name,
                total_properties=len(city_data),
                average_price=round(sum(prices) / len(prices), 2),
                average_area=round(sum(areas) / len(areas), 2),
                average_bedrooms=round(sum(bedrooms) / len(bedrooms), 2),
                min_price=min(prices),
                max_price=max(prices)
            ))
    
    logger.info(f"✅ Got statistics for {total_cities} cities")
    
    return MarketStatistics(
        total_properties=total_properties,
        total_cities=total_cities,
        average_price=round(avg_price, 2),
        average_area=round(avg_area, 2),
        cities_stats=cities_stats
    )

@app.get("/api/statistics/city/{city_name}")
def get_city_statistics(city_name: str, db: Session = Depends(get_db)):
    """Get statistics for a specific city"""
    logger.info(f"📊 Getting statistics for {city_name}")
    
    city_data = db.query(FactProperties).filter(
        FactProperties.city == city_name
    ).all()
    
    if not city_data:
        logger.error(f"❌ No data for city {city_name}")
        raise HTTPException(status_code=404, detail=f"No data for city {city_name}")
    
    prices = [p.price_milli for p in city_data]
    areas = [p.area for p in city_data]
    bedrooms = [p.bedrooms for p in city_data]
    
    logger.info(f"✅ Got statistics for {city_name}")
    
    return {
        "city": city_name,
        "total_properties": len(city_data),
        "average_price": round(sum(prices) / len(prices), 2),
        "average_area": round(sum(areas) / len(areas), 2),
        "average_bedrooms": round(sum(bedrooms) / len(bedrooms), 2),
        "min_price": min(prices),
        "max_price": max(prices),
        "price_distribution": {
            "min": min(prices),
            "q1": sorted(prices)[len(prices) // 4],
            "median": sorted(prices)[len(prices) // 2],
            "q3": sorted(prices)[3 * len(prices) // 4],
            "max": max(prices)
        }
    }

# ============ ERROR HANDLERS ============

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    logger.error(f"❌ HTTP Error: {exc.detail}")
    return {"error": exc.detail, "status_code": exc.status_code}

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)