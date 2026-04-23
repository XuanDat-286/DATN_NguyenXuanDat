# src/etl/etl_pipeline.py

import pandas as pd
import yaml
import sys
from pathlib import Path
from typing import Dict, Tuple

sys.path.append('.')
from src.utils.logger_config import setup_logger
from src.etl.data_loader import DataLoader
from src.etl.data_cleaner import DataCleaner
from src.etl.data_transformer import DataTransformer

logger = setup_logger()

class ETLPipeline:
    """Main ETL Pipeline"""
    
    def __init__(self, config_path='config/config.yaml'):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.processed_path = self.config['data']['processed_path']
        
        # Create processed folder if not exists
        Path(self.processed_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"✅ ETL Pipeline initialized")
    
    def extract(self, fact_nrows=None) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """Step 1: Extract data from CSV"""
        logger.info("\n" + "="*60)
        logger.info("📥 STEP 1: EXTRACT")
        logger.info("="*60)
        
        loader = DataLoader('config/config.yaml')
        dimensions, fact = loader.load_all_data(fact_nrows=fact_nrows)
        
        return dimensions, fact
    
    def clean(self, dimensions: Dict[str, pd.DataFrame], fact: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """Step 2: Clean data"""
        logger.info("\n" + "="*60)
        logger.info("🧹 STEP 2: CLEAN")
        logger.info("="*60)
        
        cleaner = DataCleaner(self.config)
        
        dimensions = cleaner.clean_dimensions(dimensions)
        fact = cleaner.clean_fact_table(fact)
        
        cleaner.generate_quality_report(dimensions, fact)
        
        return dimensions, fact
    
    def transform(self, dimensions: Dict[str, pd.DataFrame], fact: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """Step 3: Transform data"""
        logger.info("\n" + "="*60)
        logger.info("🔄 STEP 3: TRANSFORM")
        logger.info("="*60)
        
        transformer = DataTransformer(self.config)
        dimensions, fact = transformer.transform_all(dimensions, fact)
        
        return dimensions, fact
    
    def save(self, dimensions: Dict[str, pd.DataFrame], fact: pd.DataFrame) -> None:
        """Step 4: Save processed data to CSV"""
        logger.info("\n" + "="*60)
        logger.info("💾 STEP 4: SAVE")
        logger.info("="*60)
        
        # Save dimensions
        logger.info("\n📁 Saving dimension tables...")
        for dim_name, dim_df in dimensions.items():
            file_path = f"{self.processed_path}dim_{dim_name}.csv"
            dim_df.to_csv(file_path, index=False, encoding='utf-8')
            logger.info(f"  ✅ Saved: {file_path} ({len(dim_df)} records)")
        
        # Save fact table
        fact_path = f"{self.processed_path}fact_properties.csv"
        fact.to_csv(fact_path, index=False, encoding='utf-8')
        logger.info(f"\n  ✅ Saved: {fact_path} ({len(fact)} records)")
        
        logger.info("="*60)
    
    def run(self, fact_nrows=None) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        """Run complete ETL pipeline"""
        logger.info("\n\n" + "="*60)
        logger.info("🚀 STARTING ETL PIPELINE")
        logger.info("="*60)
        
        # Extract
        dimensions, fact = self.extract(fact_nrows=fact_nrows)
        
        # Clean
        dimensions, fact = self.clean(dimensions, fact)
        
        # Transform
        dimensions, fact = self.transform(dimensions, fact)
        
        # Save
        self.save(dimensions, fact)
        
        logger.info("\n" + "="*60)
        logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*60 + "\n")
        
        return dimensions, fact


if __name__ == "__main__":
    pipeline = ETLPipeline('config/config.yaml')
    dimensions, fact = pipeline.run()
    
    # Print summary
    logger.info("\n📊 FINAL SUMMARY:")
    logger.info("="*60)
    for dim_name, dim_df in dimensions.items():
        logger.info(f"  - {dim_name}: {len(dim_df)} records")
    logger.info(f"  - fact: {len(fact)} records")
    logger.info("="*60)