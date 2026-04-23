# src/etl/data_cleaner.py

import pandas as pd
import numpy as np
import sys
from typing import Dict, Tuple

sys.path.append('.')
from src.utils.logger_config import setup_logger

logger = setup_logger()

class DataCleaner:
    """Xử lý và làm sạch dữ liệu"""
    
    def __init__(self, config):
        self.config = config
        self.quality_config = config.get('data_quality', {})
        logger.info("✅ DataCleaner initialized")
    
    def check_missing_values(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Kiểm tra missing values"""
        logger.info(f"\n📊 Checking missing values for {name}...")
        
        missing = df.isnull().sum()
        missing_pct = (missing / len(df) * 100).round(2)
        
        missing_info = pd.DataFrame({
            'Column': df.columns,
            'Missing Count': missing.values,
            'Missing %': missing_pct.values
        })
        
        missing_info = missing_info[missing_info['Missing Count'] > 0].sort_values('Missing %', ascending=False)
        
        if len(missing_info) > 0:
            logger.info(f"\n{name} - Missing values found:")
            for _, row in missing_info.iterrows():
                logger.info(f"  - {row['Column']}: {row['Missing Count']} ({row['Missing %']:.2f}%)")
        else:
            logger.info(f"  ✅ No missing values found!")
        
        return missing_info
    
    def remove_missing_values(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Loại bỏ rows có missing values"""
        initial_count = len(df)
        df_cleaned = df.dropna()
        removed_count = initial_count - len(df_cleaned)
        
        if removed_count > 0:
            logger.info(f"  ⚠️  Removed {removed_count} rows with missing values ({removed_count/initial_count*100:.2f}%)")
        
        return df_cleaned
    
    def detect_outliers(self, df: pd.DataFrame, numeric_cols: list, name: str) -> Dict[str, list]:
        """Detect outliers using IQR method"""
        logger.info(f"\n🔍 Detecting outliers for {name}...")
        
        outliers = {}
        threshold = self.quality_config.get('outlier_threshold', 1.5)
        
        for col in numeric_cols:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
                outlier_count = outlier_mask.sum()
                
                if outlier_count > 0:
                    outliers[col] = {
                        'count': outlier_count,
                        'percentage': (outlier_count / len(df) * 100),
                        'lower_bound': lower_bound,
                        'upper_bound': upper_bound
                    }
                    logger.info(f"  ⚠️  {col}: {outlier_count} outliers ({outlier_count/len(df)*100:.2f}%)")
        
        if len(outliers) == 0:
            logger.info(f"  ✅ No outliers detected!")
        
        return outliers
    
    def remove_duplicates(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """Loại bỏ duplicate rows"""
        logger.info(f"\n🔄 Checking duplicates for {name}...")
        
        initial_count = len(df)
        df_cleaned = df.drop_duplicates()
        removed_count = initial_count - len(df_cleaned)
        
        if removed_count > 0:
            logger.info(f"  ⚠️  Removed {removed_count} duplicate rows")
        else:
            logger.info(f"  ✅ No duplicates found!")
        
        return df_cleaned
    
    def clean_fact_table(self, fact: pd.DataFrame) -> pd.DataFrame:
        """Clean fact table"""
        logger.info("\n" + "="*60)
        logger.info("🧹 CLEANING FACT TABLE")
        logger.info("="*60)
        
        # Check missing values
        self.check_missing_values(fact, "Fact Table")
        
        # Remove duplicates
        fact = self.remove_duplicates(fact, "Fact Table")
        
        # Detect outliers
        numeric_cols = self.quality_config.get('numeric_columns', [])
        self.detect_outliers(fact, numeric_cols, "Fact Table")
        
        logger.info(f"\n✅ Fact table cleaned | Final records: {len(fact):,}")
        logger.info("="*60)
        
        return fact
    
    def clean_dimensions(self, dimensions: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Clean all dimension tables"""
        logger.info("\n" + "="*60)
        logger.info("🧹 CLEANING DIMENSION TABLES")
        logger.info("="*60)
        
        cleaned_dims = {}
        
        for dim_name, dim_df in dimensions.items():
            logger.info(f"\n📍 Cleaning {dim_name}...")
            
            # Check missing values
            self.check_missing_values(dim_df, dim_name)
            
            # Remove duplicates
            dim_df = self.remove_duplicates(dim_df, dim_name)
            
            cleaned_dims[dim_name] = dim_df
            logger.info(f"  ✅ {dim_name} cleaned | Final records: {len(dim_df):,}")
        
        logger.info("="*60)
        return cleaned_dims
    
    def generate_quality_report(self, dimensions: Dict[str, pd.DataFrame], fact: pd.DataFrame) -> None:
        """Tạo báo cáo chất lượng dữ liệu"""
        logger.info("\n" + "="*60)
        logger.info("📈 DATA QUALITY REPORT")
        logger.info("="*60)
        
        logger.info("\n📊 Dimension Tables:")
        for dim_name, dim_df in dimensions.items():
            logger.info(f"  - {dim_name}: {len(dim_df):,} records | {dim_df.shape[1]} columns")
        
        logger.info(f"\n📊 Fact Table:")
        logger.info(f"  - Records: {len(fact):,}")
        logger.info(f"  - Columns: {fact.shape[1]}")
        logger.info(f"  - Memory usage: {fact.memory_usage().sum() / 1024**2:.2f} MB")
        
        logger.info("="*60)


if __name__ == "__main__":
    import yaml
    
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    from src.etl.data_loader import DataLoader
    
    loader = DataLoader('config/config.yaml')
    dimensions, fact = loader.load_all_data()
    
    cleaner = DataCleaner(config)
    
    # Clean dimensions
    dimensions_cleaned = cleaner.clean_dimensions(dimensions)
    
    # Clean fact table
    fact_cleaned = cleaner.clean_fact_table(fact)
    
    # Generate report
    cleaner.generate_quality_report(dimensions_cleaned, fact_cleaned)