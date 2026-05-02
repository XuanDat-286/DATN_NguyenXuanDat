"""
Feature Engineering cho dữ liệu bất động sản
- Xử lý đặc trưng: khu vực, diện tích, loại hình, thời gian
- Tạo derived features
- Mã hóa categorical variables
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import json
import os
import pickle
from typing import Tuple, Optional, List, Dict


class FeatureEngineer:
    """
    Xử lý feature engineering cho dữ liệu bất động sản
    """
    
    def __init__(self, config_path: str = None):
        """
        Khởi tạo FeatureEngineer
        
        Args:
            config_path: Đường dẫn đến file config JSON
        """
        self.config = self._load_config(config_path)
        self.label_encoders = {}
        self.scaler = StandardScaler()
        self.preprocessor = None
        self.feature_names = None
        
    def _load_config(self, config_path: str) -> dict:
        """Load cấu hình feature từ file JSON"""
        default_config = {
            "numeric_features": [
                "area", "bedrooms_num", "bathrooms_num",
                "year", "month", "quarter", "day_of_week"
            ],
            "categorical_features": [
                "location_id", "type_id", "trans_id", "city"
            ],
            "derived_features": [
                "area_per_bedroom",
                "luxury_score",
                "floor_area_ratio"
            ],
            "target": "price_per_m2",
            "test_size": 0.2,
            "random_state": 42
        }
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            default_config.update(config)
        
        return default_config
    
    def create_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tạo các derived features
        
        Args:
            df: DataFrame đầu vào
            
        Returns:
            DataFrame với derived features
        """
        df = df.copy()
        
        # Diện tích trung bình trên mỗi phòng ngủ
        df['area_per_bedroom'] = np.where(
            df['bedrooms_num'] > 0,
            df['area'] / df['bedrooms_num'],
            df['area']
        )
        
        # Luxury score: kết hợp diện tích và số phòng
        df['luxury_score'] = (
            df['area'] * 0.3 + 
            df['bedrooms_num'] * 0.25 + 
            df['bathrooms_num'] * 0.25
        ) / 100
        
        # Mật độ xây dựng (giả định)
        df['floor_area_ratio'] = df['area'] / 100
        
        # Phân loại diện tích
        df['area_category'] = pd.cut(
            df['area'],
            bins=[0, 30, 60, 100, 200, float('inf')],
            labels=['Nhỏ', 'Trung bình nhỏ', 'Trung bình', 'Lớn', 'Rất lớn']
        )
        
        # Phân loại giá/m2
        if 'price_per_m2' in df.columns:
            df['price_category'] = pd.cut(
                df['price_per_m2'],
                bins=[0, 0.1, 0.2, 0.3, 0.5, float('inf')],
                labels=['Rất thấp', 'Thấp', 'Trung bình', 'Cao', 'Rất cao']
            )
        
        return df
    
    def merge_dimension_data(self, 
                             fact_df: pd.DataFrame,
                             dim_location: pd.DataFrame,
                             dim_property_type: pd.DataFrame,
                             dim_transaction_type: pd.DataFrame,
                             dim_time: pd.DataFrame) -> pd.DataFrame:
        """
        Merge fact table với các dimension tables
        
        Args:
            fact_df: Fact table (fact_properties)
            dim_location: Dimension location
            dim_property_type: Dimension property type
            dim_transaction_type: Dimension transaction type
            dim_time: Dimension time
            
        Returns:
            DataFrame đã merge
        """
        df = fact_df.copy()
        
        # Merge với dim_location
        if 'location_id' in df.columns and 'location_id' in dim_location.columns:
            df = df.merge(
                dim_location[['location_id', 'district', 'city']],
                on='location_id',
                how='left'
            )
        
        # Merge với dim_property_type
        if 'type_id' in df.columns and 'type_id' in dim_property_type.columns:
            df = df.merge(
                dim_property_type,
                on='type_id',
                how='left'
            )
        
        # Merge với dim_transaction_type
        if 'trans_id' in df.columns and 'trans_id' in dim_transaction_type.columns:
            df = df.merge(
                dim_transaction_type,
                on='trans_id',
                how='left'
            )
        
        # Merge với dim_time
        if 'time_id' in df.columns and 'time_id' in dim_time.columns:
            time_cols = ['time_id', 'year', 'month', 'quarter', 'day_of_week']
            available_cols = [c for c in time_cols if c in dim_time.columns]
            df = df.merge(
                dim_time[available_cols],
                on='time_id',
                how='left'
            )
        
        return df
    
    def build_preprocessor(self, 
                           numeric_cols: List[str] = None,
                           categorical_cols: List[str] = None) -> ColumnTransformer:
        """
        Xây dựng preprocessor pipeline
        
        Args:
            numeric_cols: Danh sách cột numeric
            categorical_cols: Danh sách cột categorical
            
        Returns:
            ColumnTransformer
        """
        if numeric_cols is None:
            numeric_cols = self.config.get('numeric_features', ['area'])
        
        if categorical_cols is None:
            categorical_cols = self.config.get('categorical_features', ['location_id'])
        
        # Lọc các cột tồn tại
        numeric_cols = [c for c in numeric_cols if c != 'price_per_m2']
        
        numeric_transformer = Pipeline(steps=[
            ('scaler', StandardScaler())
        ])
        
        categorical_transformer = Pipeline(steps=[
            ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
        ])
        
        self.preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_cols),
                ('cat', categorical_transformer, categorical_cols)
            ],
            remainder='drop'
        )
        
        return self.preprocessor
    
    def fit_transform(self, 
                      df: pd.DataFrame,
                      target_col: str = None) -> Tuple[np.ndarray, Optional[np.ndarray]]:
        """
        Fit và transform dữ liệu
        
        Args:
            df: DataFrame đầu vào
            target_col: Tên cột target (nếu có)
            
        Returns:
            Tuple (X_transformed, y)
        """
        # Tạo derived features
        df = self.create_derived_features(df)
        
        # Xác định feature columns
        numeric_features = [
            'area', 'bedrooms_num', 'bathrooms_num',
            'area_per_bedroom', 'luxury_score', 'floor_area_ratio'
        ]
        
        categorical_features = ['location_id', 'type_id', 'trans_id']
        
        # Thêm time features nếu có
        time_features = ['year', 'month', 'quarter', 'day_of_week']
        for col in time_features:
            if col in df.columns:
                numeric_features.append(col)
        
        # Thêm city nếu có
        if 'city' in df.columns:
            categorical_features.append('city')
        
        # Lọc features tồn tại trong df
        numeric_features = [c for c in numeric_features if c in df.columns]
        categorical_features = [c for c in categorical_features if c in df.columns]
        
        # Build preprocessor
        self.build_preprocessor(numeric_features, categorical_features)
        
        # Fit transform
        X = self.preprocessor.fit_transform(df[numeric_features + categorical_features])
        
        # Lấy feature names
        self.feature_names = self._get_feature_names()
        
        # Xử lý target
        y = None
        if target_col and target_col in df.columns:
            y = df[target_col].values
        
        return X, y
    
    def transform(self, df: pd.DataFrame) -> np.ndarray:
        """
        Transform dữ liệu mới sử dụng preprocessor đã fit
        
        Args:
            df: DataFrame đầu vào
            
        Returns:
            Mảng numpy đã transform
        """
        if self.preprocessor is None:
            raise ValueError("Preprocessor chưa được fit. Gọi fit_transform() trước.")
        
        df = self.create_derived_features(df)
        return self.preprocessor.transform(df)
    
    def _get_feature_names(self) -> List[str]:
        """Lấy tên các features sau khi transform"""
        if self.preprocessor is None:
            return []
        
        feature_names = []
        
        # Numeric features
        num_names = self.preprocessor.named_transformers_['num'].get_feature_names_out()
        feature_names.extend(num_names)
        
        # Categorical features
        if hasattr(self.preprocessor.named_transformers_['cat'].named_steps['onehot'], 'get_feature_names_out'):
            cat_names = self.preprocessor.named_transformers_['cat'].named_steps['onehot'].get_feature_names_out()
            feature_names.extend(cat_names)
        
        return list(feature_names)
    
    def save_preprocessor(self, path: str):
        """Lưu preprocessor vào file"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump({
                'preprocessor': self.preprocessor,
                'feature_names': self.feature_names,
                'config': self.config
            }, f)
        print(f"[✓] Preprocessor đã lưu tại: {path}")
    
    def load_preprocessor(self, path: str):
        """Load preprocessor từ file"""
        with open(path, 'rb') as f:
            data = pickle.load(f)
        self.preprocessor = data['preprocessor']
        self.feature_names = data['feature_names']
        self.config = data['config']
        print(f"[✓] Preprocessor đã load từ: {path}")


if __name__ == "__main__":
    # Test
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
    
    # Load dữ liệu
    df = pd.read_csv('data/processed/fact_properties.csv')
    dim_loc = pd.read_csv('data/processed/dim_location.csv')
    dim_type = pd.read_csv('data/processed/dim_property_type.csv')
    dim_trans = pd.read_csv('data/processed/dim_transaction_type.csv')
    dim_time = pd.read_csv('data/processed/dim_time.csv')
    
    # Khởi tạo FeatureEngineer
    fe = FeatureEngineer()
    
    # Merge
    df_merged = fe.merge_dimension_data(df, dim_loc, dim_type, dim_trans, dim_time)
    print(f"Shape after merge: {df_merged.shape}")
    
    # Fit transform
    X, y = fe.fit_transform(df_merged, target_col='price_per_m2')
    print(f"X shape: {X.shape}")
    print(f"y shape: {y.shape}")