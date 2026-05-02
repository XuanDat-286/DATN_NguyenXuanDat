"""
Module đánh giá mô hình Machine Learning
- Tính toán MAE, RMSE, R², MAPE
- So sánh hiệu năng các model
- Vẽ biểu đồ đánh giá
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    mean_absolute_percentage_error
)
from typing import Dict, List, Tuple, Any
import os
import json
from datetime import datetime


class ModelEvaluator:
    """
    Đánh giá và so sánh hiệu năng các mô hình ML
    """
    
    def __init__(self):
        """Khởi tạo evaluator"""
        self.results = {}
        self.comparison_df = None
    
    def evaluate_model(self, 
                       y_true: np.ndarray, 
                       y_pred: np.ndarray,
                       model_name: str = "Model") -> Dict[str, float]:
        """
        Đánh giá một model với các chỉ số
        
        Args:
            y_true: Giá trị thực tế
            y_pred: Giá trị dự đoán
            model_name: Tên model
            
        Returns:
            Dictionary chứa các metrics
        """
        # Các chỉ số đánh giá
        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)
        
        # MAPE - xử lý chia cho 0
        try:
            mape = mean_absolute_percentage_error(y_true, y_pred) * 100
        except:
            # Nếu có giá trị 0, tính MAPE thủ công
            mask = y_true != 0
            if mask.sum() > 0:
                mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
            else:
                mape = np.nan
        
        # Các chỉ số bổ sung
        # Explained Variance
        from sklearn.metrics import explained_variance_score
        evs = explained_variance_score(y_true, y_pred)
        
        # Max Error
        max_error = np.max(np.abs(y_true - y_pred))
        
        # Median Absolute Error
        med_ae = np.median(np.abs(y_true - y_pred))
        
        metrics = {
            'MAE': round(mae, 4),
            'RMSE': round(rmse, 4),
            'R2': round(r2, 4),
            'MAPE': round(mape, 2) if not np.isnan(mape) else None,
            'Explained_Variance': round(evs, 4),
            'Max_Error': round(max_error, 4),
            'Median_AE': round(med_ae, 4),
            'Mean_y_true': round(np.mean(y_true), 4),
            'Std_y_true': round(np.std(y_true), 4),
            'Mean_y_pred': round(np.mean(y_pred), 4),
            'Std_y_pred': round(np.std(y_pred), 4)
        }
        
        self.results[model_name] = metrics
        
        return metrics
    
    def compare_models(self, 
                       models_predictions: Dict[str, Tuple[np.ndarray, np.ndarray]]) -> pd.DataFrame:
        """
        So sánh nhiều model
        
        Args:
            models_predictions: Dict {model_name: (y_true, y_pred)}
            
        Returns:
            DataFrame so sánh
        """
        all_metrics = []
        
        for model_name, (y_true, y_pred) in models_predictions.items():
            metrics = self.evaluate_model(y_true, y_pred, model_name)
            metrics['Model'] = model_name
            all_metrics.append(metrics)
        
        self.comparison_df = pd.DataFrame(all_metrics)
        self.comparison_df = self.comparison_df.set_index('Model')
        
        return self.comparison_df
    
    def print_comparison_report(self):
        """In báo cáo so sánh"""
        if self.comparison_df is None:
            print("Chưa có kết quả so sánh. Gọi compare_models() trước.")
            return
        
        print("\n" + "="*80)
        print("BÁO CÁO SO SÁNH HIỆU NĂNG CÁC MÔ HÌNH")
        print("="*80)
        
        # Bảng chính
        main_metrics = ['MAE', 'RMSE', 'R2', 'MAPE']
        available_metrics = [m for m in main_metrics if m in self.comparison_df.columns]
        
        print(f"\n{'Model':<25}", end="")
        for metric in available_metrics:
            print(f"{metric:>12}", end="")
        print()
        print("-"*80)
        
        for model_name, row in self.comparison_df.iterrows():
            print(f"{model_name:<25}", end="")
            for metric in available_metrics:
                value = row[metric]
                if metric == 'MAPE' and value is not None:
                    print(f"{value:>10.2f}%", end="  ")
                else:
                    print(f"{value:>12.4f}", end="")
            print()
        
        # Đánh dấu model tốt nhất
        if 'RMSE' in self.comparison_df.columns:
            best_model = self.comparison_df['RMSE'].idxmin()
            print(f"\n→ Model tốt nhất (RMSE thấp nhất): {best_model}")
        
        if 'R2' in self.comparison_df.columns:
            best_r2_model = self.comparison_df['R2'].idxmax()
            print(f"→ Model tốt nhất (R² cao nhất): {best_r2_model}")
        
        print("="*80)
    
    def plot_predictions_vs_actual(self, 
                                    y_true: np.ndarray, 
                                    y_pred: np.ndarray,
                                    model_name: str = "Model",
                                    save_path: str = None):
        """
        Vẽ biểu đồ so sánh giá trị thực tế và dự đoán
        
        Args:
            y_true: Giá trị thực tế
            y_pred: Giá trị dự đoán
            model_name: Tên model
            save_path: Đường dẫn lưu biểu đồ
        """
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        # Scatter plot
        ax1 = axes[0]
        ax1.scatter(y_true, y_pred, alpha=0.6, c='steelblue', edgecolors='white', s=50)
        
        # Line y = x
        min_val = min(y_true.min(), y_pred.min())
        max_val = max(y_true.max(), y_pred.max())
        ax1.plot([min_val, max_val], [min_val, max_val], 'r--', lw=2, label='Dự đoán hoàn hảo')
        
        ax1.set_xlabel('Giá trị thực tế', fontsize=12)
        ax1.set_ylabel('Giá trị dự đoán', fontsize=12)
        ax1.set_title(f'{model_name}: Thực tế vs Dự đoán', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Residual plot
        ax2 = axes[1]
        residuals = y_true - y_pred
        ax2.scatter(y_pred, residuals, alpha=0.6, c='coral', edgecolors='white', s=50)
        ax2.axhline(y=0, color='r', linestyle='--', lw=2)
        
        ax2.set_xlabel('Giá trị dự đoán', fontsize=12)
        ax2.set_ylabel('Residuals (Thực tế - Dự đoán)', fontsize=12)
        ax2.set_title(f'{model_name}: Residual Plot', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"[✓] Đã lưu biểu đồ tại: {save_path}")
        
        plt.show()
    
    def plot_feature_importance(self,
                                 model: Any,
                                 feature_names: List[str],
                                 model_name: str = "Model",
                                 top_n: int = 15,
                                 save_path: str = None):
        """
        Vẽ biểu đồ feature importance
        
        Args:
            model: Model đã huấn luyện
            feature_names: Tên các features
            model_name: Tên model
            top_n: Số features hiển thị
            save_path: Đường dẫn lưu biểu đồ
        """
        # Kiểm tra model có feature_importances_ không
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
        elif hasattr(model, 'coef_'):
            importances = np.abs(model.coef_)
            if len(importances.shape) > 1:
                importances = importances[0]
        else:
            print(f"Model {model_name} không hỗ trợ feature importance")
            return
        
        # Tạo DataFrame
        importance_df = pd.DataFrame({
            'feature': feature_names[:len(importances)],
            'importance': importances
        }).sort_values('importance', ascending=False)
        
        # Lấy top N
        importance_df = importance_df.head(top_n)
        
        # Vẽ
        fig, ax = plt.subplots(figsize=(10, 8))
        
        colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(importance_df)))
        bars = ax.barh(range(len(importance_df)), importance_df['importance'], color=colors)
        
        ax.set_yticks(range(len(importance_df)))
        ax.set_yticklabels(importance_df['feature'])
        ax.invert_yaxis()
        ax.set_xlabel('Mức độ quan trọng', fontsize=12)
        ax.set_title(f'{model_name}: Top {top_n} Feature Importance', fontsize=14, fontweight='bold')
        
        # Thêm giá trị
        for bar, value in zip(bars, importance_df['importance']):
            ax.text(bar.get_width() + 0.002, bar.get_y() + bar.get_height()/2,
                   f'{value:.4f}', va='center', fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"[✓] Đã lưu biểu đồ feature importance tại: {save_path}")
        
        plt.show()
    
    def plot_models_comparison(self, save_path: str = None):
        """
        Vẽ biểu đồ so sánh các model
        
        Args:
            save_path: Đường dẫn lưu biểu đồ
        """
        if self.comparison_df is None:
            print("Chưa có kết quả so sánh.")
            return
        
        metrics_to_plot = ['MAE', 'RMSE', 'R2']
        available_metrics = [m for m in metrics_to_plot if m in self.comparison_df.columns]
        
        fig, axes = plt.subplots(1, len(available_metrics), figsize=(6*len(available_metrics), 5))
        
        if len(available_metrics) == 1:
            axes = [axes]
        
        colors = ['#2196F3', '#4CAF50', '#FF9800']
        
        for ax, metric, color in zip(axes, available_metrics, colors):
            values = self.comparison_df[metric]
            bars = ax.bar(values.index, values.values, color=color, alpha=0.8)
            
            # Thêm giá trị trên bar
            for bar, value in zip(bars, values.values):
                if metric == 'R2':
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                           f'{value:.4f}', ha='center', fontweight='bold')
                else:
                    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.001,
                           f'{value:.4f}', ha='center', fontweight='bold')
            
            ax.set_title(metric, fontsize=14, fontweight='bold')
            ax.set_ylabel(metric)
            ax.tick_params(axis='x', rotation=45)
            
            if metric == 'R2':
                ax.set_ylim(0, 1.1)
        
        plt.suptitle('So sánh hiệu năng các mô hình', fontsize=16, fontweight='bold', y=1.02)
        plt.tight_layout()
        
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
           