"""
02_TRAIN_EVALUATE.PY - HUẤN LUYỆN & ĐÁNH GIÁ MÔ HÌNH ML
==========================================================
TUẦN 7-8: Pipeline huấn luyện mô hình Machine Learning

Chức năng:
1. Load dữ liệu từ data/processed/
2. Feature Engineering
3. Chia Train/Validation/Test
4. Huấn luyện 3 model:
   - Linear Regression (baseline)
   - Random Forest
   - XGBoost
5. Grid Search tối ưu tham số
6. Đánh giá: MAE, RMSE, R², MAPE
7. So sánh & chọn model tốt nhất
8. Vẽ biểu đồ
9. Lưu model (.joblib)
"""

import pandas as pd
import numpy as np
import os
import sys
import json
import pickle
import warnings
from datetime import datetime
from typing import Dict, Tuple, Any

# ML libraries
from sklearn.model_selection import train_test_split, GridSearchCV, cross_val_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import (
    mean_absolute_error, 
    mean_squared_error, 
    r2_score,
    mean_absolute_percentage_error,
    explained_variance_score
)
import xgboost as xgb

# Visualization
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings('ignore')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


# ============================================================
# CONFIG
# ============================================================
CONFIG = {
    # Đường dẫn
    'data_dir': 'data/processed',
    'model_dir': 'data/models',
    'report_dir': 'data/reports',
    'plot_dir': 'data/reports/plots',
    
    # Dữ liệu
    'target_col': 'price_per_m2',
    'test_size': 0.2,
    'val_size': 0.1,  # Từ training
    'random_state': 42,
    
    # Features
    'numeric_features': [
        'area', 'bedrooms_num', 'bathrooms_num',
        'area_per_bedroom', 'total_rooms',
        'year', 'month', 'quarter', 'day_of_week'
    ],
    'categorical_features': [
        'city', 'type_name', 'trans_name',
        'area_category', 'season', 'region'
    ],
    
    # Training
    'cv_folds': 5,
    'use_grid_search': False,
    'n_jobs': -1,  # Dùng tất cả CPU
}


# ============================================================
# BƯỚC 1: LOAD DỮ LIỆU
# ============================================================
def load_data(data_dir: str) -> pd.DataFrame:
    """Load master file"""
    print("\n" + "="*70)
    print("  [BƯỚC 1] LOAD DỮ LIỆU")
    print("="*70)
    
    filepath = os.path.join(data_dir, 'master_flat_properties_for_ml.csv')
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Không tìm thấy: {filepath}")
    
    df = pd.read_csv(filepath)
    print(f"  [✓] Đã load: {df.shape[0]:,} dòng, {df.shape[1]} cột")
    
    return df


# ============================================================
# BƯỚC 2: FEATURE ENGINEERING & PREPROCESSING
# ============================================================
def preprocess_data(df: pd.DataFrame, target_col: str) -> Tuple:
    """
    Tiền xử lý dữ liệu
    
    Returns:
        X_train, X_val, X_test, y_train, y_val, y_test, preprocessor
    """
    print("\n" + "="*70)
    print("  [BƯỚC 2] TIỀN XỬ LÝ DỮ LIỆU")
    print("="*70)
    
    df = df.copy()
    
    # ----------------------------------------------------------
    # 2.1. Xác định features
    # ----------------------------------------------------------
    # Lọc numeric features có trong df
    numeric_features = [c for c in CONFIG['numeric_features'] if c in df.columns]
    # Lọc categorical features có trong df
    categorical_features = [c for c in CONFIG['categorical_features'] if c in df.columns]
    
    print(f"  Numeric features ({len(numeric_features)}): {numeric_features}")
    print(f"  Categorical features ({len(categorical_features)}): {categorical_features}")
    
    # Tất cả features
    feature_cols = numeric_features + categorical_features
    
    # ----------------------------------------------------------
    # 2.2. Xử lý target
    # ----------------------------------------------------------
    if target_col not in df.columns:
        raise ValueError(f"Không tìm thấy target '{target_col}' trong dữ liệu!")
    
    # Xóa NaN ở target
    df = df.dropna(subset=[target_col])
    
    # Log transform nếu target bị skew
    y = df[target_col].values
    skewness = pd.Series(y).skew()
    use_log = abs(skewness) > 2
    
    if use_log:
        print(f"  [✓] Log-transform target (skewness={skewness:.2f} > 2)")
        # Thêm 1 để tránh log(0)
        min_val = y.min()
        if min_val <= 0:
            offset = abs(min_val) + 1
            y = np.log1p(y + offset)
        else:
            y = np.log1p(y)
        target_transformed = True
    else:
        print(f"  [~] Giữ nguyên target (skewness={skewness:.2f})")
        target_transformed = False
    
    # ----------------------------------------------------------
    # 2.3. Chuẩn bị X
    # ----------------------------------------------------------
    X = df[feature_cols].copy()
    
    # Điền NaN cho categorical
    for col in categorical_features:
        if col in X.columns:
            X[col] = X[col].fillna('Không xác định').astype(str)
    
    # Điền NaN cho numeric
    for col in numeric_features:
        if col in X.columns:
            X[col] = X[col].fillna(0)
    
    # ----------------------------------------------------------
    # 2.4. Build preprocessor
    # ----------------------------------------------------------
    numeric_transformer = Pipeline(steps=[
        ('scaler', StandardScaler())
    ])
    
    categorical_transformer = Pipeline(steps=[
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False, max_categories=50))
    ])
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ],
        remainder='drop'
    )
    
    # ----------------------------------------------------------
    # 2.5. Chia dữ liệu
    # ----------------------------------------------------------
    # Split test
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=CONFIG['test_size'], 
        random_state=CONFIG['random_state']
    )
    
    # Split validation từ temp
    val_ratio = CONFIG['val_size'] / (1 - CONFIG['test_size'])
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=val_ratio,
        random_state=CONFIG['random_state']
    )
    
    # Fit preprocessor trên training
    X_train_processed = preprocessor.fit_transform(X_train)
    X_val_processed = preprocessor.transform(X_val)
    X_test_processed = preprocessor.transform(X_test)
    
    print(f"\n  Kích thước dữ liệu:")
    print(f"  - Train: {X_train.shape[0]:,} mẫu ({X_train.shape[0]/len(df)*100:.0f}%)")
    print(f"  - Validation: {X_val.shape[0]:,} mẫu ({X_val.shape[0]/len(df)*100:.0f}%)")
    print(f"  - Test: {X_test.shape[0]:,} mẫu ({X_test.shape[0]/len(df)*100:.0f}%)")
    print(f"  - Features sau transform: {X_train_processed.shape[1]}")
    
    # Lưu thông tin
    metadata = {
        'numeric_features': numeric_features,
        'categorical_features': categorical_features,
        'use_log_transform': use_log,
        'target_col': target_col,
    }
    
    return (X_train_processed, X_val_processed, X_test_processed,
            y_train, y_val, y_test,
            preprocessor, metadata)


# ============================================================
# BƯỚC 3: ĐỊNH NGHĨA MODELS
# ============================================================
def get_models() -> Dict[str, Any]:
    """Định nghĩa các model và tham số Grid Search"""
    
    models = {
        'LinearRegression': {
            'model': LinearRegression(),
            'params': {}  # Không có tham số để tune
        },
        'RandomForest': {
            'model': RandomForestRegressor(random_state=CONFIG['random_state']),
            'params': {
                'n_estimators': [100, 200, 300],
                'max_depth': [10, 20, 30, None],
                'min_samples_split': [2, 5, 10],
                'min_samples_leaf': [1, 2, 4],
                'max_features': ['sqrt', 'log2']
            }
        },
        'XGBoost': {
            'model': xgb.XGBRegressor(
                random_state=CONFIG['random_state'],
                verbosity=0,
                n_jobs=CONFIG['n_jobs']
            ),
            'params': {
                'n_estimators': [100, 200, 300],
                'max_depth': [3, 5, 7, 10],
                'learning_rate': [0.01, 0.05, 0.1],
                'subsample': [0.8, 1.0],
                'colsample_bytree': [0.8, 1.0],
                'reg_alpha': [0, 0.1, 1],
                'reg_lambda': [1, 2]
            }
        },
        'GradientBoosting': {
            'model': GradientBoostingRegressor(random_state=CONFIG['random_state']),
            'params': {
                'n_estimators': [100, 200],
                'max_depth': [3, 5, 7],
                'learning_rate': [0.05, 0.1, 0.2],
                'min_samples_split': [2, 5],
                'min_samples_leaf': [1, 2]
            }
        }
    }
    
    return models


# ============================================================
# BƯỚC 4: HUẤN LUYỆN
# ============================================================
def train_models(X_train, y_train, X_val, y_val) -> Dict[str, Any]:
    """
    Huấn luyện tất cả model
    
    Returns:
        Dictionary {model_name: {model, best_params, metrics, ...}}
    """
    print("\n" + "="*70)
    print("  [BƯỚC 4] HUẤN LUYỆN MÔ HÌNH")
    print("="*70)
    
    models_config = get_models()
    results = {}
    
    for name, config in models_config.items():
        print(f"\n  {'─'*50}")
        print(f"  [{name}]")
        
        model = config['model']
        params = config['params']
        
        if CONFIG['use_grid_search'] and params:
            # Grid Search
            print(f"    Grid Search với {CONFIG['cv_folds']} folds...")
            print(f"    Số tổ hợp tham số: {np.prod([len(v) for v in params.values()])}")
            
            grid = GridSearchCV(
                model, params,
                cv=CONFIG['cv_folds'],
                scoring='neg_root_mean_squared_error',
                n_jobs=CONFIG['n_jobs'],
                verbose=1
            )
            grid.fit(X_train, y_train)
            
            best_model = grid.best_estimator_
            best_params = grid.best_params_
            cv_rmse = -grid.best_score_
            
            print(f"    Best params: {best_params}")
            print(f"    CV RMSE: {cv_rmse:.4f}")
        else:
            # Train không Grid Search
            model.fit(X_train, y_train)
            best_model = model
            best_params = model.get_params()
            
            # CV score
            cv_scores = cross_val_score(
                model, X_train, y_train,
                cv=CONFIG['cv_folds'],
                scoring='neg_root_mean_squared_error'
            )
            cv_rmse = -cv_scores.mean()
            print(f"    CV RMSE (5-fold): {cv_rmse:.4f} (±{cv_scores.std():.4f})")
        
        # Đánh giá trên validation set
        y_pred_val = best_model.predict(X_val)
        val_rmse = np.sqrt(mean_squared_error(y_val, y_pred_val))
        val_mae = mean_absolute_error(y_val, y_pred_val)
        val_r2 = r2_score(y_val, y_pred_val)
        
        # MAPE
        try:
            val_mape = mean_absolute_percentage_error(y_val, y_pred_val) * 100
        except:
            mask = y_val != 0
            val_mape = np.mean(np.abs((y_val[mask] - y_pred_val[mask]) / y_val[mask])) * 100
        
        print(f"    Validation:")
        print(f"    - RMSE: {val_rmse:.4f}")
        print(f"    - MAE:  {val_mae:.4f}")
        print(f"    - R²:   {val_r2:.4f}")
        print(f"    - MAPE: {val_mape:.2f}%")
        
        results[name] = {
            'model': best_model,
            'best_params': best_params,
            'cv_rmse': cv_rmse,
            'val_rmse': val_rmse,
            'val_mae': val_mae,
            'val_r2': val_r2,
            'val_mape': val_mape
        }
    
    return results


# ============================================================
# BƯỚC 5: ĐÁNH GIÁ TRÊN TEST SET
# ============================================================
def evaluate_on_test(results: Dict, X_test, y_test) -> pd.DataFrame:
    """
    Đánh giá tất cả model trên tập test
    
    Returns:
        DataFrame so sánh
    """
    print("\n" + "="*70)
    print("  [BƯỚC 5] ĐÁNH GIÁ TRÊN TEST SET")
    print("="*70)
    
    eval_results = []
    
    for name, info in results.items():
        model = info['model']
        y_pred = model.predict(X_test)
        
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        evs = explained_variance_score(y_test, y_pred)
        
        try:
            mape = mean_absolute_percentage_error(y_test, y_pred) * 100
        except:
            mask = y_test != 0
            mape = np.mean(np.abs((y_test[mask] - y_pred[mask]) / y_test[mask])) * 100
        
        eval_results.append({
            'Model': name,
            'RMSE': rmse,
            'MAE': mae,
            'R²': r2,
            'MAPE': mape,           # ← SỬA Ở ĐÂY
            'Explained Var': evs,
            'CV RMSE': info['cv_rmse'],
            'Val RMSE': info['val_rmse']
        })
    
    df_results = pd.DataFrame(eval_results).set_index('Model')
    
    # Tìm best model
    best_model_name = df_results['RMSE'].idxmin()
    
    print(f"\n  KẾT QUẢ SO SÁNH TRÊN TEST SET:")
    print(f"  {'='*80}")
    print(f"  {'Model':<20} {'RMSE':>10} {'MAE':>10} {'R²':>10} {'MAPE':>10}")
    print(f"  {'-'*60}")
    
    for name, row in df_results.iterrows():
        marker = ' ← BEST' if name == best_model_name else ''
        print(f"  {name:<20} {row['RMSE']:>10.4f} {row['MAE']:>10.4f} "
              f"{row['R²']:>10.4f} {row['MAPE']:>9.2f}%{marker}")    # ← SỬA Ở ĐÂY
    
    print(f"  {'='*80}")
    
    results['_best_model'] = best_model_name
    results['_test_results'] = df_results
    
    return df_results, best_model_name


# ============================================================
# BƯỚC 6: FEATURE IMPORTANCE
# ============================================================
def analyze_feature_importance(results: Dict, preprocessor, metadata: Dict, plot_dir: str):
    """Phân tích và vẽ feature importance"""
    print("\n" + "="*70)
    print("  [BƯỚC 6] PHÂN TÍCH FEATURE IMPORTANCE")
    print("="*70)
    
    # Lấy feature names
    numeric_features = metadata['numeric_features']
    categorical_features = metadata['categorical_features']
    
    # Lấy tên từ OneHotEncoder
    ohe = preprocessor.named_transformers_['cat'].named_steps['onehot']
    cat_feature_names = ohe.get_feature_names_out(categorical_features).tolist()
    
    all_feature_names = numeric_features + cat_feature_names
    
    # Tìm model có feature_importances_
    for name, info in results.items():
        if name.startswith('_'):
            continue
        
        model = info['model']
        
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
            
            # Đảm bảo độ dài khớp
            if len(importances) == len(all_feature_names):
                importance_df = pd.DataFrame({
                    'feature': all_feature_names,
                    'importance': importances
                }).sort_values('importance', ascending=False)
                
                print(f"\n  [{name}] TOP 15 FEATURES:")
                for i, row in importance_df.head(15).iterrows():
                    bar = '█' * int(row['importance'] / importance_df['importance'].max() * 40)
                    print(f"    {row['feature']:<40} {row['importance']:.4f} {bar}")
                
                # Vẽ
                fig, ax = plt.subplots(figsize=(10, 8))
                top_features = importance_df.head(15)
                
                colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(top_features)))
                ax.barh(range(len(top_features)), top_features['importance'], color=colors)
                ax.set_yticks(range(len(top_features)))
                ax.set_yticklabels(top_features['feature'])
                ax.invert_yaxis()
                ax.set_xlabel('Importance')
                ax.set_title(f'{name} - Feature Importance')
                plt.tight_layout()
                
                os.makedirs(plot_dir, exist_ok=True)
                plt.savefig(os.path.join(plot_dir, f'{name}_feature_importance.png'), dpi=150, bbox_inches='tight')
                plt.close()
                
                break


# ============================================================
# BƯỚC 7: VẼ BIỂU ĐỒ
# ============================================================
def plot_results(results: Dict, X_test, y_test, best_model_name: str, plot_dir: str):
    """Vẽ các biểu đồ đánh giá"""
    print("\n" + "="*70)
    print("  [BƯỚC 7] VẼ BIỂU ĐỒ")
    print("="*70)
    
    os.makedirs(plot_dir, exist_ok=True)
    
    # ----------------------------------------------------------
    # 7.1. Actual vs Predicted cho best model
    # ----------------------------------------------------------
    best_model = results[best_model_name]['model']
    y_pred = best_model.predict(X_test)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # Scatter
    ax1 = axes[0]
    ax1.scatter(y_test, y_pred, alpha=0.4, c='steelblue', edgecolors='white', s=30)
    
    min_val = min(y_test.min(), y_pred.min())
    max_val = max(y_test.max(), y_pred.max())
    ax1.plot([min_val, max_val], [min_val, max_val], 'r--', lw=2, label='Hoàn hảo')
    
    ax1.set_xlabel('Giá trị thực tế')
    ax1.set_ylabel('Giá trị dự đoán')
    ax1.set_title(f'{best_model_name}: Actual vs Predicted')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Residuals
    ax2 = axes[1]
    residuals = y_test - y_pred
    ax2.scatter(y_pred, residuals, alpha=0.4, c='coral', edgecolors='white', s=30)
    ax2.axhline(y=0, color='r', linestyle='--', lw=2)
    ax2.set_xlabel('Giá trị dự đoán')
    ax2.set_ylabel('Residuals')
    ax2.set_title(f'{best_model_name}: Residual Plot')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(plot_dir, 'best_model_predictions.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [✓] best_model_predictions.png")
    
    # ----------------------------------------------------------
    # 7.2. So sánh model
    # ----------------------------------------------------------
    if '_test_results' in results:
        df_comp = results['_test_results']
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        
        metrics = ['RMSE', 'MAE', 'R²']
        colors = ['#2196F3', '#4CAF50', '#FF9800']
        
        for ax, metric, color in zip(axes, metrics, colors):
            if metric in df_comp.columns:
                values = df_comp[metric]
                bars = ax.bar(values.index, values.values, color=color, alpha=0.8)
                
                for bar, val in zip(bars, values.values):
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                           f'{val:.4f}', ha='center', va='bottom', fontweight='bold', fontsize=9)
                
                ax.set_title(metric, fontsize=14, fontweight='bold')
                ax.tick_params(axis='x', rotation=45)
                
                if metric == 'R²':
                    ax.set_ylim(0, 1.1)
        
        plt.suptitle('So sánh hiệu năng các mô hình', fontsize=16, fontweight='bold')
        plt.tight_layout()
        plt.savefig(os.path.join(plot_dir, 'models_comparison.png'), dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  [✓] models_comparison.png")
    
    # ----------------------------------------------------------
    # 7.3. Residual distribution
    # ----------------------------------------------------------
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(residuals, bins=50, color='steelblue', edgecolor='white', alpha=0.7, density=True)
    ax.axvline(x=0, color='r', linestyle='--', lw=2)
    ax.set_xlabel('Residuals')
    ax.set_ylabel('Density')
    ax.set_title(f'{best_model_name}: Residual Distribution')
    plt.tight_layout()
    plt.savefig(os.path.join(plot_dir, 'residual_distribution.png'), dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  [✓] residual_distribution.png")


# ============================================================
# BƯỚC 8: LƯU MODEL
# ============================================================
def save_models(results: Dict, preprocessor, metadata: Dict, model_dir: str):
    """Lưu model, preprocessor, metadata"""
    print("\n" + "="*70)
    print("  [BƯỚC 8] LƯU MODEL")
    print("="*70)
    
    os.makedirs(model_dir, exist_ok=True)
    
    # Lưu từng model
    for name, info in results.items():
        if name.startswith('_'):
            continue
        
        model_path = os.path.join(model_dir, f'house_price_model_{name}.joblib')
        with open(model_path, 'wb') as f:
            pickle.dump(info['model'], f)
        print(f"  [✓] {model_path}")
    
    # Lưu preprocessor
    preprocessor_path = os.path.join(model_dir, 'preprocessor.joblib')
    with open(preprocessor_path, 'wb') as f:
        pickle.dump(preprocessor, f)
    print(f"  [✓] {preprocessor_path}")
    
    # Lưu metadata
    best_model = results.get('_best_model', '')
    
    model_metadata = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'best_model': best_model,
        'target_col': CONFIG['target_col'],
        'numeric_features': metadata['numeric_features'],
        'categorical_features': metadata['categorical_features'],
        'use_log_transform': metadata['use_log_transform'],
        'models': {}
    }
    
    for name, info in results.items():
        if name.startswith('_'):
            continue
        model_metadata['models'][name] = {
            'best_params': info['best_params'],
            'cv_rmse': float(info['cv_rmse']),
            'val_rmse': float(info['val_rmse']),
            'val_mae': float(info['val_mae']),
            'val_r2': float(info['val_r2']),
            'val_mape': float(info['val_mape'])
        }
    
    # Thêm test results
    if '_test_results' in results:
        model_metadata['test_results'] = results['_test_results'].to_dict()
    
    metadata_path = os.path.join(model_dir, 'model_metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(model_metadata, f, indent=2, ensure_ascii=False, default=str)
    print(f"  [✓] {metadata_path}")
    
    print(f"\n  → Tất cả model đã lưu tại: {os.path.abspath(model_dir)}")


# ============================================================
# BƯỚC 9: BÁO CÁO TỔNG HỢP
# ============================================================
def save_training_report(results: Dict, metadata: Dict, report_dir: str):
    """Lưu báo cáo training"""
    
    report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'best_model': results.get('_best_model', ''),
        'target_variable': CONFIG['target_col'],
        'log_transform': metadata['use_log_transform'],
        'data_split': {
            'test_size': CONFIG['test_size'],
            'val_size': CONFIG['val_size'],
            'cv_folds': CONFIG['cv_folds']
        },
        'models': {}
    }
    
    for name, info in results.items():
        if name.startswith('_'):
            continue
        report['models'][name] = {
            'best_params': info['best_params'],
            'cv_rmse': float(info['cv_rmse']),
            'val_rmse': float(info['val_rmse']),
            'val_mae': float(info['val_mae']),
            'val_r2': float(info['val_r2']),
            'val_mape': float(info['val_mape'])
        }
    
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, 'training_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n  [✓] Báo cáo: {report_path}")


# ============================================================
# MAIN
# ============================================================
def main():
    """Pipeline chính"""
    
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "   TUẦN 7-8: HUẤN LUYỆN MÔ HÌNH MACHINE LEARNING".center(68) + "█")
    print("█" + f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    # BƯỚC 1: Load data
    df = load_data(CONFIG['data_dir'])
    
    # BƯỚC 2: Preprocess
    (X_train, X_val, X_test,
     y_train, y_val, y_test,
     preprocessor, metadata) = preprocess_data(df, CONFIG['target_col'])
    
    # BƯỚC 3-4: Train
    results = train_models(X_train, y_train, X_val, y_val)
    
    # BƯỚC 5: Evaluate on test
    df_test_results, best_model_name = evaluate_on_test(results, X_test, y_test)
    
    # BƯỚC 6: Feature importance
    analyze_feature_importance(results, preprocessor, metadata, CONFIG['plot_dir'])
    
    # BƯỚC 7: Plot
    plot_results(results, X_test, y_test, best_model_name, CONFIG['plot_dir'])
    
    # BƯỚC 8: Save models
    save_models(results, preprocessor, metadata, CONFIG['model_dir'])
    
    # BƯỚC 9: Report
    save_training_report(results, metadata, CONFIG['report_dir'])
    
    # TỔNG KẾT
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "   HOÀN TẤT HUẤN LUYỆN!".center(68) + "█")
    print("█" + f"   Best Model: {best_model_name}".center(68) + "█")
    
    if best_model_name in results:
        best = results[best_model_name]
        print("█" + f"   RMSE: {best['val_rmse']:.4f} | MAE: {best['val_mae']:.4f} | R²: {best['val_r2']:.4f}".center(68) + "█")
    
    print("█" + " "*68 + "█")
    print("█" + f"   Models: {CONFIG['model_dir']}".center(68) + "█")
    print("█" + f"   Plots:  {CONFIG['plot_dir']}".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    print(f"\n  Tiếp theo:")
    print(f"  - Chạy API: uvicorn src.api.main:app --reload")
    print(f"  - Truy cập: http://localhost:8000/docs")


if __name__ == "__main__":
    main()