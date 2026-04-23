# src/etl/data_transformer.py

import pandas as pd
import numpy as np
import sys
from typing import Dict

sys.path.append('.')
from src.utils.logger_config import setup_logger

logger = setup_logger()

class DataTransformer:
    """Transform và chuẩn hóa dữ liệu"""
    
    def __init__(self, config):
        self.config = config
        logger.info("✅ DataTransformer initialized")
    
    def transform_location(self, location_df: pd.DataFrame) -> pd.DataFrame:
        """Transform location dimension"""
        logger.info("\n📍 Transforming location table...")
        
        # Lowercase tên district và city
        location_df['district'] = location_df['district'].str.strip().str.lower()
        location_df['city'] = location_df['city'].str.strip().str.lower()
        
        # Tạo location_name
        location_df['location_name'] = location_df['district'] + ', ' + location_df['city']
        
        logger.info(f"  ✅ Location table transformed")
        return location_df
    
    def transform_time(self, time_df: pd.DataFrame) -> pd.DataFrame:
        """Transform time dimension"""
        logger.info("\n⏰ Transforming time table...")
        
        # Convert date to datetime
        time_df['date'] = pd.to_datetime(time_df['date'])
        
        # Ensure numeric columns
        time_df['day'] = time_df['day'].astype(int)
        time_df['month'] = time_df['month'].astype(int)
        time_df['year'] = time_df['year'].astype(int)
        time_df['quarter'] = time_df['quarter'].astype(int)
        
        logger.info(f"  ✅ Time table transformed")
        return time_df
    
    def transform_property_type(self, property_type_df: pd.DataFrame) -> pd.DataFrame:
        """Transform property type dimension"""
        logger.info("\n🏠 Transforming property_type table...")
        
        property_type_df['type_name'] = property_type_df['type_name'].str.strip().str.lower()
        
        logger.info(f"  ✅ Property type table transformed")
        return property_type_df
    
    def transform_transaction_type(self, transaction_type_df: pd.DataFrame) -> pd.DataFrame:
        """Transform transaction type dimension"""
        logger.info("\n💼 Transforming transaction_type table...")
        
        transaction_type_df['trans_name'] = transaction_type_df['trans_name'].str.strip().str.lower()
        
        logger.info(f"  ✅ Transaction type table transformed")
        return transaction_type_df
    
    def transform_fact_table(self, fact_df: pd.DataFrame, location_df: pd.DataFrame) -> pd.DataFrame:
        """Transform fact table"""
        logger.info("\n📊 Transforming fact table...")
        
        # Ensure ID columns are integers
        fact_df['property_id'] = fact_df['property_id'].astype(int)
        fact_df['time_id'] = fact_df['time_id'].astype(int)
        fact_df['location_id'] = fact_df['location_id'].astype(int)
        fact_df['type_id'] = fact_df['type_id'].astype(int)
        fact_df['trans_id'] = fact_df['trans_id'].astype(int)
        
        # Ensure numeric columns are float
        numeric_cols = ['area', 'price_milli', 'bedrooms', 'bathrooms_num']
        for col in numeric_cols:
            if col in fact_df.columns:
                fact_df[col] = pd.to_numeric(fact_df[col], errors='coerce')
        
        # Calculate price_per_m2 if not exists
        if 'price_per_m2' not in fact_df.columns and 'area' in fact_df.columns and 'price_milli' in fact_df.columns:
            fact_df['price_per_m2'] = (fact_df['price_milli'] * 1000000) / fact_df['area']
            fact_df['price_per_m2'] = fact_df['price_per_m2'].round(2)
        
        # Merge with location để có district và city
        fact_df = fact_df.merge(
            location_df[['location_id', 'district', 'city']],
            on='location_id',
            how='left'
        )
        
        # Sort by time_id
        fact_df = fact_df.sort_values('time_id').reset_index(drop=True)
        
        logger.info(f"  ✅ Fact table transformed | Final records: {len(fact_df):,}")
        return fact_df
    
    def transform_all(self, dimensions: Dict[str, pd.DataFrame], fact: pd.DataFrame) -> tuple:
        """Transform tất cả dữ liệu"""
        logger.info("\n" + "="*60)
        logger.info("🔄 TRANSFORMING DATA")
        logger.info("="*60)
        
        # Transform dimensions
        dimensions['location'] = self.transform_location(dimensions['location'])
        dimensions['time'] = self.transform_time(dimensions['time'])
        dimensions['property_type'] = self.transform_property_type(dimensions['property_type'])
        dimensions['transaction_type'] = self.transform_transaction_type(dimensions['transaction_type'])
        
        # Transform fact table
        fact = self.transform_fact_table(fact, dimensions['location'])
        
        logger.info("\n" + "="*60)
        logger.info("✅ Data transformation completed!")
        logger.info("="*60)
        
        return dimensions, fact


if __name__ == "__main__":
    import yaml
    
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    from src.etl.data_loader import DataLoader
    from src.etl.data_cleaner import DataCleaner
    
    logger.info("\n" + "="*60)
    logger.info("🚀 ETL PIPELINE TEST")
    logger.info("="*60)
    
    # Load
    loader = DataLoader('config/config.yaml')
    dimensions, fact = loader.load_all_data()
    
    # Clean
    cleaner = DataCleaner(config)
    dimensions = cleaner.clean_dimensions(dimensions)
    fact = cleaner.clean_fact_table(fact)
    
    # Transform
    transformer = DataTransformer(config)
    dimensions, fact = transformer.transform_all(dimensions, fact)
    
    # Show sample
    logger.info("\n📊 SAMPLE DATA:")
    logger.info("\nFact table sample:")
    logger.info(fact.head())