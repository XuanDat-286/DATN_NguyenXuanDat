"""
Module huấn luyện các mô hình Machine Learning
- Linear Regression
- Random Forest
- XGBoost
"""

import numpy as np
import pandas as pd
import os
import pickle
import json
from typing import Dict, Tuple, Any
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
import xgboost as xgb
import warnings
warnings.filterwarnings('ignore')


class ModelTrainer:
    """
    Huấn luyện và lưu trữ các mô hình ML
    """
    
    def __init__(self, config: dict = None):
        """
        Khởi tạo ModelTrainer
        
        Args:
            config: Cấu hình model
        """
        self.config = config or {}
        self.models = {}
        self.best_model = None
        self.best_model_name = None
        
        # Cấu hình mặc định cho từng model
        self.model_configs = {
            'linear_regression': {
                'model': LinearRegression(),
                'params': {}
            },
            'random_forest': {
                'model': RandomForestRegressor(random_state=42),
                'params': {
                    'n_estimators': [100, 200, 300, 500],
                    'max_depth': [10, 20, 30, None],
                    'min_samples_split': [2, 5, 10],
                    'min_samples_leaf': [1, 2, 4],
                    'max_features': ['sqrt', 'log2', None]
                }
            },
            'xgboost': {
                'model': xgb.XGBRegressor(random_state=42, verbosity=0),
                'params': {
                    'n_estimators': [100, 200, 300, 500],
                    'max_depth': [3, 5, 7, 10],
                    'learning_rate': [0.01, 0.05, 0.1, 0.2],
                    'subsample': [0.6, 0.8, 1.0],
                    'colsample_bytree': [0.6, 0.8, 1.0],
                    'gamma': [0, 0.1, 0.2],
                    'reg_alpha': [0, 0.1, 1],
                    'reg_lambda': [1, 1.5, 2]
                }
            }
        }
    
    def train_all_models(self,
                         X_train: np.ndarray,
                         y_train: np.ndarray,
                         X_val: np.ndarray = None,
                         y_val: np.ndarray = None,
                         use_grid_search: bool = True,
                         cv_folds: int = 5) -> Dict[str, Any]:
        """
        Huấn luyện tất cả các mô hình
        
        Args:
            X_train: Features training
            y_train: Target training
            X_val: Features validation
            y_val: Target validation
            use_grid_search: Sử dụng GridSearchCV
            cv_folds: Số folds cross-validation
            
        Returns:
            Dictionary chứa các model đã huấn luyện
        """
        results = {}
        
        for model_name, model_config in self.model_configs.items():
            print(f"\n{'='*60}")
            print(f"[TRAINING] {model_name.upper()}")
            print(f"{'='*60}")
            
            if use_grid_search and model_config['params']:
                # Grid Search
                print(f"  Grid Search với {cv_folds}-fold CV...")
                
                grid_search = GridSearchCV(
                    estimator=model_config['model'],
                    param_grid=model_config['params'],
                    cv=cv_folds,
                    scoring='neg_root_mean_squared_error',
                    n_jobs=-1,
                    verbose=1
                )
                
                grid_search.fit(X_train, y_train)
                
                model = grid_search.best_estimator_
                best_params = grid_search.best_params_
                best_score = -grid_search.best_score_
                
                print(f"  Best params: {best_params}")
                print(f"  Best CV RMSE: {best_score:.4f}")
                
            else:
                # Train đơn giản
                model = model_config['model']
                model.fit(X_train, y_train)
                best_params = model.get_params()
                best_score = None
            
            # Đánh giá trên validation set
            if X_val is not None and y_val is not None:
                from sklearn.metrics import mean_squared_error, mean_absolute_error
                y_pred = model.predict(X_val)
                val_rmse = np.sqrt(mean_squared_error(y_val, y_pred))
                val_mae = mean_absolute_error(y_val, y_pred)
                print(f"  Validation RMSE: {val_rmse:.4f}")
                print(f"  Validation MAE: {val_mae:.4f}")
            else:
                val_rmse = None
                val_mae = None
            
            # Lưu model
            self.models[model_name] = model
            
            results[model_name] = {
                'model': model,
                'best_params': best_params,
                'cv_rmse': best_score,
                'val_rmse': val_rmse,
                'val_mae': val_mae
            }
        
        # Chọn model tốt nhất
        self._select_best_model(results)
        
        return results
    
    def train_single_model(self,
                          model_name: str,
                          X_train: np.ndarray,
                          y_train: np.ndarray) -> Any:
        """
        Huấn luyện một model cụ thể
        
        Args:
            model_name: Tên model ('linear_regression', 'random_forest', 'xgboost')
            X_train: Features training
            y_train: Target training
            
        Returns:
            Model đã huấn luyện
        """
        if model_name not in self.model_configs:
            raise ValueError(f"Model '{model_name}' không được hỗ trợ. "
                           f"Chọn: {list(self.model_configs.keys())}")
        
        model_config = self.model_configs[model_name]
        model = model_config['model']
        model.fit(X_train, y_train)
        
        self.models[model_name] = model
        return model
    
    def _select_best_model(self, results: Dict[str, Any]):
        """
        Chọn model tốt nhất dựa trên validation RMSE
        """
        best_rmse = float('inf')
        best_name = None
        
        for name, result in results.items():
            if result['val_rmse'] and result['val_rmse'] < best_rmse:
                best_rmse = result['val_rmse']
                best_name = name
        
        if best_name:
            self.best_model = results[best_name]['model']
            self.best_model_name = best_name
            print(f"\n{'='*60}")
            print(f"[BEST MODEL] {best_name.upper()}")
            print(f"  Validation RMSE: {best_rmse:.4f}")
            print(f"{'='*60}")
    
    def save_models(self, output_dir: str = 'data/models'):
        """
        Lưu tất cả các model
        
        Args:
            output_dir: Thư mục lưu model
        """
        os.makedirs(output_dir, exist_ok=True)
        
        for name, model in self.models.items():
            # Lưu model
            model_path = os.path.join(output_dir, f'house_price_model_{name}.joblib')
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
            print(f"[✓] Đã lưu {name}: {model_path}")
        
        # Lưu metadata
        metadata = {
            'models': list(self.models.keys()),
            'best_model': self.best_model_name
        }
        metadata_path = os.path.join(output_dir, 'model_metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"\n[✓] Đã lưu tất cả model tại: {output_dir}")
    
    def load_models(self, model_dir: str = 'data/models'):
        """
        Load tất cả model
        
        Args:
            model_dir: Thư mục chứa model
        """
        import glob
        
        model_files = glob.glob(os.path.join(model_dir, 'house_price_model_*.joblib'))
        
        for model_path in model_files:
            name = os.path.basename(model_path).replace('house_price_model_', '').replace('.joblib', '')
            with open(model_path, 'rb') as f:
                self.models[name] = pickle.load(f)
            print(f"[✓] Đã load {name} từ {model_path}")
        
        # Load metadata
        metadata_path = os.path.join(model_dir, 'model_metadata.json')
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            if metadata.get('best_model'):
                self.best_model_name = metadata['best_model']
                self.best_model = self.models.get(self.best_model_name)
        
        print(f"[✓] Đã load {len(self.models)} model(s)")


if __name__ == "__main__":
    # Test
    from sklearn.datasets import make_regression
    
    X, y = make_regression(n_samples=1000, n_features=10, noise=0.1, random_state=42)
    
    trainer = ModelTrainer()
    results = trainer.train_all_models(X, y, use_grid_search=False)
    trainer.save_models('test_models')