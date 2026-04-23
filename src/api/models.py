from sqlalchemy import Column, Integer, String, Float, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DimLocation(Base):
    __tablename__ = "dim_location"
    
    location_id = Column(Integer, primary_key=True)
    district = Column(String(100))
    city = Column(String(100))

class DimTime(Base):
    __tablename__ = "dim_time"
    
    time_id = Column(Integer, primary_key=True)
    date = Column(Date)
    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    quarter = Column(Integer)
    day_of_week = Column(Integer)

class DimPropertyType(Base):
    __tablename__ = "dim_property_type"
    
    type_id = Column(Integer, primary_key=True)
    type_name = Column(String(100))

class DimTransactionType(Base):
    __tablename__ = "dim_transaction_type"
    
    trans_id = Column(Integer, primary_key=True)
    trans_name = Column(String(100))

class FactProperties(Base):
    __tablename__ = "fact_properties"
    
    property_id = Column(Integer, primary_key=True)
    time_id = Column(Integer, ForeignKey("dim_time.time_id"))
    location_id = Column(Integer, ForeignKey("dim_location.location_id"))
    type_id = Column(Integer, ForeignKey("dim_property_type.type_id"))
    trans_id = Column(Integer, ForeignKey("dim_transaction_type.trans_id"))
    area = Column(Float)
    price_million = Column(Float)
    price_per_m2 = Column(Float)
    bedrooms_num = Column(Integer)
    bathrooms_num = Column(Integer)

    # QUAN TRỌNG: Bạn phải thêm dòng này để main.py có thể dùng p.location.city
    location = relationship("DimLocation")