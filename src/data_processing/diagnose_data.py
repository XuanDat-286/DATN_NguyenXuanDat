"""
DIAGNOSE_DATA.PY - CHẨN ĐOÁN & KIỂM TRA CHẤT LƯỢNG DỮ LIỆU
=============================================================
Chức năng:
1. Kiểm tra file đầu ra từ 01_prepare_data.py
2. Phân tích thống kê mô tả
3. Phát hiện vấn đề: missing, outliers, imbalance
4. Đánh giá mức độ sẵn sàng cho ML
5. Xuất báo cáo
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


# ============================================================
# CONFIG
# ============================================================
CONFIG = {
    'data_dir': 'data/processed',
    'report_dir': 'data/reports',
    'target_col': 'price_per_m2',  # Cột mục tiêu cho ML
}


# ============================================================
# 1. KIỂM TRA FILE
# ============================================================
def check_files(data_dir: str) -> dict:
    """Kiểm tra sự tồn tại của các file"""
    print("\n" + "="*70)
    print("  1. KIỂM TRA FILE")
    print("="*70)
    
    files = {
        'Master (ML)': 'master_flat_properties_for_ml.csv',
        'Fact': 'fact_properties_clean.csv',
        'Dim Location': 'dim_location.csv',
        'Dim Property Type': 'dim_property_type.csv',
        'Dim Transaction': 'dim_transaction_type.csv',
        'Dim Time': 'dim_time.csv'
    }
    
    status = {}
    for name, filename in files.items():
        filepath = os.path.join(data_dir, filename)
        exists = os.path.exists(filepath)
        
        if exists:
            size_kb = os.path.getsize(filepath) / 1024
            df = pd.read_csv(filepath)
            print(f"  [✓] {name:<25} {df.shape[0]:>6} dòng | {df.shape[1]:>2} cột | {size_kb:>6.0f} KB")
            status[filename] = {'exists': True, 'rows': df.shape[0], 'cols': df.shape[1]}
        else:
            print(f"  [✗] {name:<25} KHÔNG TÌM THẤY")
            status[filename] = {'exists': False}
    
    return status


# ============================================================
# 2. PHÂN TÍCH MASTER FILE
# ============================================================
def analyze_master_file(data_dir: str) -> pd.DataFrame:
    """Phân tích chi tiết master file"""
    print("\n" + "="*70)
    print("  2. PHÂN TÍCH MASTER FILE")
    print("="*70)
    
    filepath = os.path.join(data_dir, 'master_flat_properties_for_ml.csv')
    
    if not os.path.exists(filepath):
        print("  [ERROR] Không tìm thấy master file!")
        return None
    
    df = pd.read_csv(filepath)
    
    # ----------------------------------------------------------
    # 2.1. Thông tin cơ bản
    # ----------------------------------------------------------
    print(f"\n  [2.1] THÔNG TIN CƠ BẢN:")
    print(f"    - Tổng số dòng: {df.shape[0]:,}")
    print(f"    - Tổng số cột: {df.shape[1]}")
    print(f"    - Bộ nhớ sử dụng: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    
    # Phân loại cột
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
    
    print(f"    - Cột số (numeric): {len(numeric_cols)}")
    print(f"    - Cột phân loại (categorical): {len(categorical_cols)}")
    print(f"    - Cột ngày (datetime): {len(datetime_cols)}")
    
    # ----------------------------------------------------------
    # 2.2. Missing values
    # ----------------------------------------------------------
    print(f"\n  [2.2] KIỂM TRA MISSING VALUES:")
    
    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    
    missing_df = pd.DataFrame({
        'Cột': missing.index,
        'Số lượng thiếu': missing.values,
        'Tỷ lệ (%)': missing_pct.values
    })
    missing_df = missing_df[missing_df['Số lượng thiếu'] > 0].sort_values('Số lượng thiếu', ascending=False)
    
    if missing_df.empty:
        print("    → TUYỆT VỜI! Không có giá trị thiếu nào!")
    else:
        print(f"    → Có {len(missing_df)} cột bị thiếu dữ liệu:")
        for _, row in missing_df.iterrows():
            print(f"      - {row['Cột']:<25} {row['Số lượng thiếu']:>6} ({row['Tỷ lệ (%)']:.1f}%)")
    
    # ----------------------------------------------------------
    # 2.3. Duplicates
    # ----------------------------------------------------------
    print(f"\n  [2.3] KIỂM TRA DUPLICATES:")
    
    dup_rows = df.duplicated().sum()
    if 'property_id' in df.columns:
        dup_id = df['property_id'].duplicated().sum()
    else:
        dup_id = 0
    
    print(f"    - Dòng trùng hoàn toàn: {dup_rows}")
    print(f"    - property_id trùng: {dup_id}")
    
    if dup_rows == 0 and dup_id == 0:
        print("    → Không có dữ liệu trùng lặp!")
    
    # ----------------------------------------------------------
    # 2.4. Thống kê mô tả cho cột số
    # ----------------------------------------------------------
    print(f"\n  [2.4] THỐNG KÊ MÔ TẢ (CỘT SỐ):")
    print(f"\n    {'Cột':<20} {'Min':>12} {'Q1':>12} {'Median':>12} {'Mean':>12} {'Q3':>12} {'Max':>12} {'Std':>12}")
    print(f"    {'-'*104}")
    
    important_cols = ['area', 'price_million', 'price_per_m2', 'bedrooms_num', 'bathrooms_num', 
                      'area_per_bedroom', 'total_rooms']
    
    for col in important_cols:
        if col in df.columns:
            data = df[col].dropna()
            print(f"    {col:<20} {data.min():>12.2f} {data.quantile(0.25):>12.2f} "
                  f"{data.median():>12.2f} {data.mean():>12.2f} "
                  f"{data.quantile(0.75):>12.2f} {data.max():>12.2f} {data.std():>12.2f}")
    
    # ----------------------------------------------------------
    # 2.5. Phân phối categorical
    # ----------------------------------------------------------
    print(f"\n  [2.5] PHÂN PHỐI CATEGORICAL:")
    
    # City
    if 'city' in df.columns:
        print(f"\n    TOP 10 THÀNH PHỐ:")
        city_dist = df['city'].value_counts().head(10)
        for city, count in city_dist.items():
            print(f"      {city:<25} {count:>6} ({count/len(df)*100:>5.1f}%)")
    
    # Region
    if 'region' in df.columns:
        print(f"\n    PHÂN PHỐI MIỀN:")
        region_dist = df['region'].value_counts()
        for region, count in region_dist.items():
            print(f"      {region:<25} {count:>6} ({count/len(df)*100:>5.1f}%)")
    
    # Property type
    if 'type_name' in df.columns:
        print(f"\n    LOẠI BĐS:")
        type_dist = df['type_name'].value_counts()
        for t, count in type_dist.items():
            print(f"      {str(t):<25} {count:>6} ({count/len(df)*100:>5.1f}%)")
    
    # Transaction type
    if 'trans_name' in df.columns:
        print(f"\n    LOẠI GIAO DỊCH:")
        trans_dist = df['trans_name'].value_counts()
        for t, count in trans_dist.items():
            print(f"      {str(t):<25} {count:>6} ({count/len(df)*100:>5.1f}%)")
    
    # Area category
    if 'area_category' in df.columns:
        print(f"\n    PHÂN LOẠI DIỆN TÍCH:")
        area_dist = df['area_category'].value_counts()
        for cat, count in area_dist.items():
            print(f"      {str(cat):<25} {count:>6} ({count/len(df)*100:>5.1f}%)")
    
    # Season
    if 'season' in df.columns:
        print(f"\n    MÙA:")
        season_dist = df['season'].value_counts()
        for s, count in season_dist.items():
            print(f"      {str(s):<25} {count:>6} ({count/len(df)*100:>5.1f}%)")
    
    # ----------------------------------------------------------
    # 2.6. Phân phối thời gian
    # ----------------------------------------------------------
    print(f"\n  [2.6] PHÂN PHỐI THỜI GIAN:")
    
    if 'year' in df.columns:
        print(f"\n    THEO NĂM:")
        year_dist = df['year'].value_counts().sort_index()
        for year, count in year_dist.items():
            bar = '█' * int(count / df['year'].value_counts().max() * 30)
            print(f"      {int(year)}: {count:>6} {bar}")
    
    if 'month' in df.columns:
        print(f"\n    THEO THÁNG:")
        month_dist = df['month'].value_counts().sort_index()
        for month, count in month_dist.items():
            bar = '█' * int(count / df['month'].value_counts().max() * 30)
            print(f"      Tháng {int(month):>2}: {count:>6} {bar}")
    
    if 'quarter' in df.columns:
        print(f"\n    THEO QUÝ:")
        quarter_dist = df['quarter'].value_counts().sort_index()
        for q, count in quarter_dist.items():
            bar = '█' * int(count / df['quarter'].value_counts().max() * 30)
            print(f"      Quý {int(q)}: {count:>6} {bar}")
    
    # ----------------------------------------------------------
    # 2.7. Phát hiện outliers
    # ----------------------------------------------------------
    print(f"\n  [2.7] KIỂM TRA OUTLIERS (IQR):")
    
    outlier_cols = ['price_per_m2', 'price_million', 'area']
    
    for col in outlier_cols:
        if col in df.columns:
            data = df[col].dropna()
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower = max(0, Q1 - 1.5 * IQR)
            upper = Q3 + 1.5 * IQR
            
            n_outliers = ((data < lower) | (data > upper)).sum()
            pct = n_outliers / len(data) * 100
            
            if pct > 5:
                print(f"    ⚠ {col:<20} {n_outliers:>6} outliers ({pct:.1f}%) - CÓ THỂ CẦN XỬ LÝ THÊM")
            elif pct > 1:
                print(f"    ~ {col:<20} {n_outliers:>6} outliers ({pct:.1f}%) - CHẤP NHẬN ĐƯỢC")
            else:
                print(f"    ✓ {col:<20} {n_outliers:>6} outliers ({pct:.1f}%) - TỐT")
    
    # ----------------------------------------------------------
    # 2.8. Tương quan với target
    # ----------------------------------------------------------
    target = CONFIG['target_col']
    
    if target in df.columns:
        print(f"\n  [2.8] TƯƠNG QUAN VỚI TARGET ({target}):")
        
        numeric_df = df.select_dtypes(include=[np.number])
        
        if target in numeric_df.columns:
            correlations = numeric_df.corr()[target].sort_values(key=abs, ascending=False)
            
            print(f"\n    TOP 10 FEATURES TƯƠNG QUAN MẠNH NHẤT:")
            for col, corr in correlations.head(11).items():
                if col != target:
                    bar_len = int(abs(corr) * 50)
                    direction = '→' if corr > 0 else '←'
                    bar = '█' * bar_len
                    print(f"      {col:<25} {corr:>7.3f} {direction} {bar}")
    
    return df


# ============================================================
# 3. ĐÁNH GIÁ MỨC ĐỘ SẴN SÀNG CHO ML
# ============================================================
def assess_ml_readiness(df: pd.DataFrame) -> dict:
    """Đánh giá dữ liệu đã sẵn sàng cho ML chưa"""
    print("\n" + "="*70)
    print("  3. ĐÁNH GIÁ MỨC ĐỘ SẴN SÀNG CHO ML")
    print("="*70)
    
    checks = {}
    score = 0
    max_score = 0
    
    # 3.1. Số lượng mẫu
    max_score += 1
    n_samples = df.shape[0]
    if n_samples >= 10000:
        checks['Số lượng mẫu'] = f'✓ RẤT TỐT ({n_samples:,} mẫu)'
        score += 1
    elif n_samples >= 1000:
        checks['Số lượng mẫu'] = f'~ ĐỦ DÙNG ({n_samples:,} mẫu)'
        score += 0.7
    elif n_samples >= 100:
        checks['Số lượng mẫu'] = f'⚠ ÍT ({n_samples:,} mẫu) - ML có thể không chính xác'
        score += 0.3
    else:
        checks['Số lượng mẫu'] = f'✗ QUÁ ÍT ({n_samples:,} mẫu) - Không đủ cho ML'
    
    # 3.2. Missing values
    max_score += 1
    missing_pct = df.isnull().sum().sum() / (df.shape[0] * df.shape[1]) * 100
    if missing_pct == 0:
        checks['Missing values'] = '✓ KHÔNG CÓ GIÁ TRỊ THIẾU'
        score += 1
    elif missing_pct < 5:
        checks['Missing values'] = f'~ {missing_pct:.1f}% giá trị thiếu - Chấp nhận được'
        score += 0.7
    else:
        checks['Missing values'] = f'✗ {missing_pct:.1f}% giá trị thiếu - Cần xử lý thêm'
    
    # 3.3. Số features
    max_score += 1
    n_features = len(df.select_dtypes(include=[np.number]).columns) - 1  # trừ target
    if n_features >= 10:
        checks['Số features'] = f'✓ TỐT ({n_features} features số)'
        score += 1
    elif n_features >= 5:
        checks['Số features'] = f'~ ĐỦ ({n_features} features số)'
        score += 0.7
    else:
        checks['Số features'] = f'⚠ ÍT ({n_features} features số) - Cân nhắc thêm features'
        score += 0.3
    
    # 3.4. Cân bằng dữ liệu
    max_score += 1
    if 'city' in df.columns:
        top_city_pct = df['city'].value_counts().iloc[0] / len(df) * 100
        if top_city_pct < 50:
            checks['Cân bằng thành phố'] = f'✓ TỐT (top city: {top_city_pct:.1f}%)'
            score += 1
        elif top_city_pct < 70:
            checks['Cân bằng thành phố'] = f'~ CHẤP NHẬN (top city: {top_city_pct:.1f}%)'
            score += 0.7
        else:
            checks['Cân bằng thành phố'] = f'⚠ MẤT CÂN BẰNG (top city: {top_city_pct:.1f}%)'
            score += 0.3
    
    # 3.5. Phân phối target
    max_score += 1
    target = CONFIG['target_col']
    if target in df.columns:
        skewness = df[target].skew()
        if abs(skewness) < 1:
            checks['Phân phối target'] = f'✓ TỐT (skewness: {skewness:.2f})'
            score += 1
        elif abs(skewness) < 3:
            checks['Phân phối target'] = f'~ CHẤP NHẬN (skewness: {skewness:.2f})'
            score += 0.7
        else:
            checks['Phân phối target'] = f'⚠ LỆCH (skewness: {skewness:.2f}) - Cân nhắc log transform'
            score += 0.3
    
    # Kết quả
    readiness = (score / max_score) * 100
    
    print(f"\n  Kết quả đánh giá:")
    for check, result in checks.items():
        print(f"    {result}")
    
    print(f"\n  {'='*50}")
    if readiness >= 80:
        print(f"  ĐIỂM SẴN SÀNG: {readiness:.0f}/100 → ✓ SẴN SÀNG CHO ML!")
    elif readiness >= 60:
        print(f"  ĐIỂM SẴN SÀNG: {readiness:.0f}/100 → ~ CÓ THỂ CHẠY ML (nên cải thiện thêm)")
    else:
        print(f"  ĐIỂM SẴN SÀNG: {readiness:.0f}/100 → ⚠ CẦN XỬ LÝ THÊM DỮ LIỆU")
    print(f"  {'='*50}")
    
    return {'checks': checks, 'score': score, 'max_score': max_score, 'readiness': readiness}


# ============================================================
# 4. KHUYẾN NGHỊ TIỀN XỬ LÝ CHO ML
# ============================================================
def recommend_preprocessing(df: pd.DataFrame):
    """Đưa ra khuyến nghị xử lý trước khi ML"""
    print("\n" + "="*70)
    print("  4. KHUYẾN NGHỊ TIỀN XỬ LÝ CHO ML")
    print("="*70)
    
    recommendations = []
    
    # Target
    target = CONFIG['target_col']
    if target in df.columns:
        skewness = df[target].skew()
        if abs(skewness) > 2:
            recommendations.append(f"  • Log-transform '{target}' (skewness={skewness:.2f})")
    
    # Categorical encoding
    cat_cols = df.select_dtypes(include=['object']).columns.tolist()
    high_cardinality = []
    for col in cat_cols:
        if df[col].nunique() > 50:
            high_cardinality.append(col)
    
    if high_cardinality:
        recommendations.append(f"  • Cột có cardinality cao: {high_cardinality} → Cân nhắc Target Encoding hoặc gộp nhóm")
    
    # Scaling
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if target in numeric_cols:
        numeric_cols.remove(target)
    
    # Cột cần scale
    scale_cols = []
    for col in numeric_cols:
        if col in df.columns:
            range_val = df[col].max() - df[col].min()
            if range_val > 1000:
                scale_cols.append(col)
    
    if scale_cols:
        recommendations.append(f"  • Nên StandardScaler cho: {scale_cols[:5]}...")
    
    # Feature engineering
    if 'area' in df.columns and 'price_million' not in df.columns:
        recommendations.append(f"  • Có thể tạo thêm feature tương tác (vd: area × bedrooms)")
    
    if not recommendations:
        print("\n  → Dữ liệu đã khá tốt, không cần xử lý thêm nhiều!")
    else:
        print("\n  Các khuyến nghị:")
        for rec in recommendations:
            print(rec)
    
    return recommendations


# ============================================================
# 5. XUẤT BÁO CÁO
# ============================================================
def export_report(df: pd.DataFrame, readiness: dict, recommendations: list, report_dir: str):
    """Xuất báo cáo JSON"""
    print("\n" + "="*70)
    print("  5. XUẤT BÁO CÁO")
    print("="*70)
    
    os.makedirs(report_dir, exist_ok=True)
    
    report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'dataset_summary': {
            'total_rows': int(df.shape[0]),
            'total_columns': int(df.shape[1]),
            'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': len(df.select_dtypes(include=['object']).columns),
            'missing_values': int(df.isnull().sum().sum()),
            'duplicate_rows': int(df.duplicated().sum()),
        },
        'target_variable': CONFIG['target_col'],
        'target_stats': {
            'mean': float(df[CONFIG['target_col']].mean()) if CONFIG['target_col'] in df.columns else None,
            'median': float(df[CONFIG['target_col']].median()) if CONFIG['target_col'] in df.columns else None,
            'std': float(df[CONFIG['target_col']].std()) if CONFIG['target_col'] in df.columns else None,
            'skewness': float(df[CONFIG['target_col']].skew()) if CONFIG['target_col'] in df.columns else None,
        },
        'ml_readiness': readiness,
        'recommendations': recommendations,
        'cities': df['city'].value_counts().to_dict() if 'city' in df.columns else {},
        'year_distribution': df['year'].value_counts().sort_index().to_dict() if 'year' in df.columns else {}
    }
    
    report_path = os.path.join(report_dir, 'data_diagnosis_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"  [✓] Báo cáo đã lưu: {report_path}")
    
    return report


# ============================================================
# MAIN
# ============================================================
def main():
    """Chạy chẩn đoán"""
    
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "   CHẨN ĐOÁN DỮ LIỆU - KIỂM TRA CHẤT LƯỢNG".center(68) + "█")
    print("█" + f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    data_dir = CONFIG['data_dir']
    
    # 1. Kiểm tra file
    status = check_files(data_dir)
    
    if not status.get('master_flat_properties_for_ml.csv', {}).get('exists'):
        print("\n  [DỪNG] Không tìm thấy master file!")
        print("  Hãy chạy 01_prepare_data.py trước!")
        return
    
    # 2. Phân tích master file
    df = analyze_master_file(data_dir)
    
    if df is None:
        return
    
    # 3. Đánh giá ML readiness
    readiness = assess_ml_readiness(df)
    
    # 4. Khuyến nghị
    recommendations = recommend_preprocessing(df)
    
    # 5. Xuất báo cáo
    report = export_report(df, readiness, recommendations, CONFIG['report_dir'])
    
    # Tổng kết
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "   HOÀN TẤT CHẨN ĐOÁN!".center(68) + "█")
    print("█" + f"   Dữ liệu: {df.shape[0]:,} dòng | {df.shape[1]} cột".center(68) + "█")
    print("█" + f"   Điểm sẵn sàng ML: {readiness['readiness']:.0f}/100".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    print(f"\n  Tiếp theo: python src/ml/02_train_evaluate.py")


if __name__ == "__main__":
    main()