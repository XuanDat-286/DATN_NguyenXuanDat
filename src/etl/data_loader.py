# src/etl/data_loader.py

import pandas as pd
import yaml
import sys
from typing import Dict, Tuple

sys.path.append('.')
from src.utils.logger_config import setup_logger

logger = setup_logger()

class DataLoader:
    """Load dữ liệu từ CSV files"""
    
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.raw_path = self.config['data']['raw_path']
        logger.info(f"✅ DataLoader initialized | Raw path: {self.raw_path}")
    
    def load_dimension_tables(self) -> Dict[str, pd.DataFrame]:
        """Load tất cả dimension tables"""
        dim_files = self.config['data']['dim_files']
        dimensions = {}
        
        logger.info("=" * 60)
        logger.info("🔄 LOADING DIMENSION TABLES")
        logger.info("=" * 60)
        
        for dim_name, file_name in dim_files.items():
            file_path = f"{self.raw_path}{file_name}"
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                dimensions[dim_name] = df
                logger.info(f"  ✅ {dim_name:20} | Records: {len(df):>8,}")
            except FileNotFoundError:
                logger.error(f"  ❌ File not found: {file_path}")
                raise
        
        logger.info("=" * 60)
        return dimensions
    
    def load_fact_table(self, nrows=None) -> pd.DataFrame:
        """Load fact table"""
        file_path = f"{self.raw_path}{self.config['data']['fact_file']}"
        
        logger.info("=" * 60)
        logger.info("🔄 LOADING FACT TABLE")
        logger.info("=" * 60)
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            logger.info(f"  ✅ Fact table loaded | Records: {len(df):,}")
            logger.info("=" * 60)
            return df
        except FileNotFoundError:
            logger.error(f"  ❌ File not found: {file_path}")
            raise
    
    def load_all_data(self, fact_nrows=None) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """Load tất cả dữ liệu"""
        logger.info("\n" + "=" * 60)
        logger.info("🚀 ETL PROCESS STARTED")
        logger.info("=" * 60 + "\n")
        
        dimensions = self.load_dimension_tables()
        fact = self.load_fact_table(nrows=fact_nrows)
        
        logger.info("\n✅ All data loaded successfully!")
        logger.info("=" * 60 + "\n")
        
        return dimensions, fact


if __name__ == "__main__":
    loader = DataLoader('config/config.yaml')
    dimensions, fact = loader.load_all_data()
    
    print("\n📊 Summary:")
    for dim_name, dim_df in dimensions.items():
        print(f"  - {dim_name}: {len(dim_df)} records")
    print(f"  - fact: {len(fact)} records")