# src/database/create_tables.py

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import sys

sys.path.append('.')
from src.utils.logger_config import setup_logger

logger = setup_logger()

class DatabaseManager:
    """Quản lý database"""
    
    def __init__(self, db_url):
        """
        Initialize database manager
        db_url: postgresql://username:password@localhost:5432/database_name
        """
        self.engine = create_engine(db_url)
        logger.info(f"✅ Database connection initialized")
    
    def create_tables(self):
        """Tạo tables"""
        logger.info("\n" + "="*60)
        logger.info("🗄️  CREATING TABLES")
        logger.info("="*60)
        
        # SQL statements để tạo tables
        sql_statements = [
            # Dimension tables
            """
            CREATE TABLE IF NOT EXISTS dim_location (
                location_id INTEGER PRIMARY KEY,
                district VARCHAR(100),
                city VARCHAR(100),
                location_name VARCHAR(200)
            );
            """,
            
            """
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id INTEGER PRIMARY KEY,
                date DATE,
                day INTEGER,
                month INTEGER,
                year INTEGER,
                quarter INTEGER,
                day_of_week INTEGER
            );
            """,
            
            """
            CREATE TABLE IF NOT EXISTS dim_property_type (
                type_id INTEGER PRIMARY KEY,
                type_name VARCHAR(100)
            );
            """,
            
            """
            CREATE TABLE IF NOT EXISTS dim_transaction_type (
                trans_id INTEGER PRIMARY KEY,
                trans_name VARCHAR(100)
            );
            """,
            
            # Fact table
            """
            CREATE TABLE IF NOT EXISTS fact_properties (
                property_id BIGINT,
                time_id INTEGER,
                location_id INTEGER,
                type_id INTEGER,
                trans_id INTEGER,
                area FLOAT,
                price_milli FLOAT,
                price_per_m2 FLOAT,
                bedrooms INTEGER,
                bathrooms_num INTEGER,
                district VARCHAR(100),
                city VARCHAR(100),
                PRIMARY KEY (property_id, time_id, trans_id),
                FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
                FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
                FOREIGN KEY (type_id) REFERENCES dim_property_type(type_id),
                FOREIGN KEY (trans_id) REFERENCES dim_transaction_type(trans_id)
            );
            """
        ]
        
        try:
            with self.engine.connect() as connection:
                for sql in sql_statements:
                    connection.execute(text(sql))
                    logger.info(f"  ✅ Table created")
                connection.commit()
            
            logger.info("="*60)
            logger.info("✅ All tables created successfully!")
            logger.info("="*60)
        
        except Exception as e:
            logger.error(f"❌ Error creating tables: {str(e)}")
            raise
    
    def load_data(self, dimensions, fact):
        """Load dữ liệu vào database"""
        logger.info("\n" + "="*60)
        logger.info("📥 LOADING DATA INTO DATABASE")
        logger.info("="*60)
        
        try:
            # Load dimensions
            logger.info("\n📍 Loading dimension tables...")
            for dim_name, dim_df in dimensions.items():
                table_name = f"dim_{dim_name}"
                dim_df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                logger.info(f"  ✅ {table_name}: {len(dim_df)} records")
            
            # Load fact table
            logger.info("\n📊 Loading fact table...")
            fact.to_sql('fact_properties', self.engine, if_exists='replace', index=False)
            logger.info(f"  ✅ fact_properties: {len(fact)} records")
            
            logger.info("="*60)
            logger.info("✅ Data loaded successfully!")
            logger.info("="*60)
        
        except Exception as e:
            logger.error(f"❌ Error loading data: {str(e)}")
            raise
    
    def verify_data(self):
        """Kiểm tra dữ liệu đã load"""
        logger.info("\n" + "="*60)
        logger.info("🔍 VERIFYING DATA")
        logger.info("="*60)
        
        try:
            with self.engine.connect() as connection:
                # Check dimension tables
                tables = ['dim_location', 'dim_time', 'dim_property_type', 'dim_transaction_type', 'fact_properties']
                
                for table_name in tables:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.scalar()
                    logger.info(f"  ✅ {table_name}: {count} records")
            
            logger.info("="*60)
        
        except Exception as e:
            logger.error(f"❌ Error verifying data: {str(e)}")
            raise


if __name__ == "__main__":
    import yaml
    
    with open('config/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Database URL
    db_config = config['database']
    db_url = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    
    logger.info(f"\n🔗 Connecting to database: {db_config['database']}")
    
    # Create manager
    db_manager = DatabaseManager(db_url)
    
    # Create tables
    db_manager.create_tables()
    
    # Load processed data
    logger.info("\n📂 Loading processed data...")
    processed_path = config['data']['processed_path']
    
    dimensions = {}
    for dim_name in ['location', 'time', 'property_type', 'transaction_type']:
        file_path = f"{processed_path}dim_{dim_name}.csv"
        dimensions[dim_name] = pd.read_csv(file_path)
    
    fact = pd.read_csv(f"{processed_path}fact_properties.csv")
    
    # Load data
    db_manager.load_data(dimensions, fact)
    
    # Verify
    db_manager.verify_data()
    
    logger.info("\n✅ DATABASE SETUP COMPLETED!")