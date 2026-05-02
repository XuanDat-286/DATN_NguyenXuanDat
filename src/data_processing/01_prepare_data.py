"""
01_PREPARE_DATA.PY - ETL PIPELINE (BẢN CHO 5 FILE RIÊNG BIỆT)
===============================================================
Dữ liệu đầu vào: 5 file CSV trong data/raw/
    - fact_properties.csv         (bảng sự kiện chính)
    - dim_location.csv            (thông tin vị trí)
    - dim_property_type.csv       (loại hình BĐS)
    - dim_transaction_type.csv    (loại giao dịch)
    - dim_time.csv                (thời gian)

Nhiệm vụ:
    1. Đọc 5 file riêng biệt
    2. Merge fact với 4 dim thành bảng phẳng (flat table)
    3. Làm sạch dữ liệu (thiếu, outliers, vô lý)
    4. Xuất ra data/processed/

Đầu ra:
    - master_flat_properties_for_ml.csv  (file chính cho ML)
    - fact_properties_clean.csv          (fact đã làm sạch)
    - dim_*.csv                          (dimension đã chuẩn hóa)
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


# ============================================================
# CONFIG
# ============================================================
CONFIG = {
    # Đường dẫn
    'raw_data_dir': 'data/raw',
    'processed_data_dir': 'data/processed',
    
    # Tên file đầu vào
    'files': {
        'fact': 'fact_properties.csv',
        'location': 'dim_location.csv',
        'property_type': 'dim_property_type.csv',
        'transaction_type': 'dim_transaction_type.csv',
        'time': 'dim_time.csv'
    },
    
    # Lọc thành phố (None = tất cả, 'Hồ Chí Minh' hoặc 'Hà Nội')
    'target_city': None,
    
    # Xử lý outliers
    'remove_outliers': True,
    'outlier_threshold': 1.5,
    
    # Giá trị tối thiểu hợp lý
    'min_area': 10,            # Diện tích tối thiểu (m²)
    'min_price': 50,           # Giá tối thiểu (triệu đồng)
    'max_price_per_m2': 500,   # Giá/m² tối đa hợp lý (triệu/m²)
}


# ============================================================
# BƯỚC 1: ĐỌC 5 FILE DỮ LIỆU
# ============================================================
def load_all_files(data_dir: str) -> dict:
    """
    Đọc 5 file CSV: fact + 4 dimension
    
    Args:
        data_dir: Thư mục chứa file raw
        
    Returns:
        Dictionary {tên: DataFrame}
    """
    print("\n" + "="*70)
    print("  [BƯỚC 1] ĐỌC 5 FILE DỮ LIỆU")
    print("="*70)
    
    data = {}
    
    for key, filename in CONFIG['files'].items():
        filepath = os.path.join(data_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"  [✗] {filename}: KHÔNG TÌM THẤY!")
            continue
        
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
            print(f"  [✓] {filename:<35} {df.shape[0]:>6} dòng | {df.shape[1]:>2} cột")
            print(f"      Cột: {list(df.columns)}")
            data[key] = df
        except Exception as e:
            print(f"  [✗] {filename}: Lỗi - {e}")
    
    return data


# ============================================================
# BƯỚC 2: MERGE 5 FILE THÀNH BẢNG PHẲNG
# ============================================================
def merge_to_flat_table(data: dict) -> pd.DataFrame:
    """
    Merge fact_properties với 4 dim thành bảng phẳng
    
    Star Schema:
        fact_properties (trung tâm)
            ├── JOIN dim_location ON location_id
            ├── JOIN dim_property_type ON type_id
            ├── JOIN dim_transaction_type ON trans_id
            └── JOIN dim_time ON time_id
    
    Args:
        data: Dictionary chứa 5 DataFrame
        
    Returns:
        DataFrame đã merge đầy đủ thông tin
    """
    print("\n" + "="*70)
    print("  [BƯỚC 2] MERGE 5 FILE → BẢNG PHẲNG (FLAT TABLE)")
    print("="*70)
    
    if 'fact' not in data:
        print("  [ERROR] Không có file fact_properties!")
        return None
    
    # Bắt đầu từ fact table
    df = data['fact'].copy()
    initial_rows = df.shape[0]
    initial_cols = df.shape[1]
    
    print(f"\n  Bắt đầu: fact_properties ({initial_rows} dòng, {initial_cols} cột)")
    
    # Danh sách merge
    merge_plan = [
        ('location', 'location_id', ['district', 'city']),
        ('property_type', 'type_id', ['type_name']),
        ('transaction_type', 'trans_id', ['trans_name']),
        ('time', 'time_id', ['date', 'day', 'month', 'year', 'quarter', 'day_of_week'])
    ]
    
    for dim_key, join_key, useful_cols in merge_plan:
        if dim_key in data:
            dim_df = data[dim_key].copy()
            
            # Chỉ lấy cột cần thiết (tránh trùng lặp)
            cols_to_take = [join_key] + [c for c in useful_cols if c in dim_df.columns]
            cols_to_take = [c for c in cols_to_take if c in dim_df.columns]
            
            # Merge
            df = df.merge(
                dim_df[cols_to_take],
                on=join_key,
                how='left'
            )
            
            added_cols = len(cols_to_take) - 1  # trừ cột join
            print(f"  [✓] Merge dim_{dim_key}: +{added_cols} cột (district, city, ...)")
        else:
            print(f"  [~] dim_{dim_key}: không có file, bỏ qua")
    
    final_rows = df.shape[0]
    final_cols = df.shape[1]
    
    print(f"\n  Kết quả merge:")
    print(f"  - Số dòng: {initial_rows} → {final_rows}")
    print(f"  - Số cột: {initial_cols} → {final_cols}")
    print(f"  - Các cột: {list(df.columns)}")
    
    return df


# ============================================================
# BƯỚC 3: LÀM SẠCH DỮ LIỆU
# ============================================================
def clean_merged_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Làm sạch dữ liệu sau khi merge
    
    Args:
        df: DataFrame đã merge
        
    Returns:
        DataFrame sạch
    """
    print("\n" + "="*70)
    print("  [BƯỚC 3] LÀM SẠCH DỮ LIỆU")
    print("="*70)
    
    df = df.copy()
    initial_rows = df.shape[0]
    
    # ----------------------------------------------------------
    # 3.1. Xóa dòng trùng lặp
    # ----------------------------------------------------------
    before = df.shape[0]
    df = df.drop_duplicates()
    print(f"\n  [3.1] Xóa dòng trùng: {before - df.shape[0]} dòng")
    
    # Xóa trùng property_id (giữ dòng đầu)
    if 'property_id' in df.columns:
        before = df.shape[0]
        df = df.drop_duplicates(subset=['property_id'], keep='first')
        print(f"  [3.1] Xóa property_id trùng: {before - df.shape[0]} dòng")
    
    # ----------------------------------------------------------
    # 3.2. Xử lý giá trị thiếu
    # ----------------------------------------------------------
    print(f"\n  [3.2] Xử lý giá trị thiếu:")
    
    # Các cột số: điền 0
    fill_zero_cols = ['bedrooms_num', 'bathrooms_num']
    for col in fill_zero_cols:
        if col in df.columns:
            missing = df[col].isna().sum()
            df[col] = df[col].fillna(0).astype(int)
            if missing > 0:
                print(f"    - {col}: {missing} giá trị thiếu → điền 0")
    
    # Các cột chuỗi: điền 'Không xác định'
    fill_unknown_cols = ['district', 'city', 'type_name', 'trans_name']
    for col in fill_unknown_cols:
        if col in df.columns:
            missing = df[col].isna().sum()
            df[col] = df[col].fillna('Không xác định')
            if missing > 0:
                print(f"    - {col}: {missing} giá trị thiếu → 'Không xác định'")
    
    # ----------------------------------------------------------
    # 3.3. Chuẩn hóa kiểu dữ liệu
    # ----------------------------------------------------------
    print(f"\n  [3.3] Chuẩn hóa kiểu dữ liệu:")
    
    # property_id → int
    if 'property_id' in df.columns:
        df['property_id'] = pd.to_numeric(df['property_id'], errors='coerce').fillna(0).astype(int)
    
    # area → float, > min_area
    if 'area' in df.columns:
        df['area'] = pd.to_numeric(df['area'], errors='coerce')
        before = df.shape[0]
        df = df[df['area'] >= CONFIG['min_area']]
        print(f"    - area < {CONFIG['min_area']}m²: xóa {before - df.shape[0]} dòng")
    
    # price_million → float, > min_price
    if 'price_million' in df.columns:
        df['price_million'] = pd.to_numeric(df['price_million'], errors='coerce')
        before = df.shape[0]
        df = df[df['price_million'] >= CONFIG['min_price']]
        print(f"    - price < {CONFIG['min_price']} triệu: xóa {before - df.shape[0]} dòng")
    
    # price_per_m2 → float, tính lại nếu cần
    if 'price_per_m2' in df.columns:
        df['price_per_m2'] = pd.to_numeric(df['price_per_m2'], errors='coerce')
        
        # Nếu price_per_m2 quá lớn hoặc NaN → tính lại
        before = df.shape[0]
        df = df[df['price_per_m2'] <= CONFIG['max_price_per_m2']]
        print(f"    - price_per_m2 > {CONFIG['max_price_per_m2']}: xóa {before - df.shape[0]} dòng")
    
    # Tính lại price_per_m2 cho các dòng có area và price
    if 'area' in df.columns and 'price_million' in df.columns:
        mask = df['price_per_m2'].isna() | (df['price_per_m2'] == 0)
        df.loc[mask, 'price_per_m2'] = df.loc[mask, 'price_million'] / df.loc[mask, 'area']
    
    # Các cột ID → int
    id_cols = ['time_id', 'location_id', 'type_id', 'trans_id']
    for col in id_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # bedrooms_num, bathrooms_num → int
    for col in ['bedrooms_num', 'bathrooms_num']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    print(f"    - Đã chuẩn hóa xong kiểu dữ liệu")
    
    # ----------------------------------------------------------
    # 3.4. Xóa outliers
    # ----------------------------------------------------------
    if CONFIG['remove_outliers']:
        df = remove_outliers_safe(df)
    
    # ----------------------------------------------------------
    # 3.5. Xóa NaN ở cột quan trọng
    # ----------------------------------------------------------
    critical_cols = ['area', 'price_million', 'price_per_m2']
    for col in critical_cols:
        if col in df.columns:
            before = df.shape[0]
            df = df.dropna(subset=[col])
            print(f"    - Xóa NaN {col}: {before - df.shape[0]} dòng")
    
    # ----------------------------------------------------------
    # Tổng kết
    # ----------------------------------------------------------
    final_rows = df.shape[0]
    print(f"\n  {'='*50}")
    print(f"  TỔNG KẾT LÀM SẠCH:")
    print(f"  {initial_rows} → {final_rows} dòng (đã xóa {initial_rows - final_rows})")
    print(f"  {'='*50}")
    
    return df


def remove_outliers_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Xóa outliers an toàn (đảm bảo cận dưới không âm)
    """
    print(f"\n  [3.4] Xóa outliers (IQR, threshold={CONFIG['outlier_threshold']}):")
    
    # Chỉ áp dụng cho price_per_m2 và price_million (không áp dụng area vì dễ xóa sai)
    outlier_cols = ['price_per_m2', 'price_million']
    
    for col in outlier_cols:
        if col in df.columns:
            data = df[col].dropna()
            
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            
            lower = Q1 - CONFIG['outlier_threshold'] * IQR
            upper = Q3 + CONFIG['outlier_threshold'] * IQR
            
            # Đảm bảo cận dưới không âm
            if lower < 0:
                lower = data.quantile(0.01)  # Dùng percentil 1%
            
            before = df.shape[0]
            df = df[(df[col] >= lower) & (df[col] <= upper)]
            removed = before - df.shape[0]
            
            if removed > 0:
                print(f"    - {col}: xóa {removed} outliers ({removed/before*100:.1f}%)")
                print(f"      Ngưỡng: [{lower:.2f}, {upper:.2f}]")
    
    return df


# ============================================================
# BƯỚC 4: LỌC THEO THÀNH PHỐ
# ============================================================
def filter_by_city(df: pd.DataFrame, target_city: str = None) -> pd.DataFrame:
    """
    Lọc dữ liệu theo thành phố
    """
    print("\n" + "="*70)
    print("  [BƯỚC 4] LỌC THEO THÀNH PHỐ")
    print("="*70)
    
    if target_city is None:
        print(f"\n  Target city: TẤT CẢ ({df.shape[0]} dòng)")
        
        if 'city' in df.columns:
            city_counts = df['city'].value_counts()
            print(f"\n  Phân phối:")
            for city, count in city_counts.items():
                print(f"    - {city}: {count} ({count/df.shape[0]*100:.1f}%)")
        return df
    
    if 'city' not in df.columns:
        print(f"  [WARNING] Không có cột 'city', không thể lọc")
        return df
    
    before = df.shape[0]
    df = df[df['city'] == target_city]
    after = df.shape[0]
    
    print(f"\n  Lọc '{target_city}': {before} → {after} dòng")
    
    if after == 0:
        print(f"  [CẢNH BÁO] Không có dữ liệu cho '{target_city}'!")
        cities = df['city'].unique() if 'city' in df.columns else []
        print(f"  Các thành phố có: {list(cities)}")
    
    return df


# ============================================================
# BƯỚC 5: TẠO THÊM FEATURES HỮU ÍCH
# ============================================================
def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tạo thêm features cho ML
    
    Args:
        df: DataFrame sạch
        
    Returns:
        DataFrame có thêm cột features
    """
    print("\n" + "="*70)
    print("  [BƯỚC 5] TẠO THÊM FEATURES")
    print("="*70)
    
    df = df.copy()
    new_features = []
    
    # 5.1. Diện tích/phòng ngủ
    if 'area' in df.columns and 'bedrooms_num' in df.columns:
        df['area_per_bedroom'] = np.where(
            df['bedrooms_num'] > 0,
            df['area'] / df['bedrooms_num'],
            df['area']
        )
        new_features.append('area_per_bedroom')
    
    # 5.2. Tổng số phòng
    if 'bedrooms_num' in df.columns and 'bathrooms_num' in df.columns:
        df['total_rooms'] = df['bedrooms_num'] + df['bathrooms_num']
        new_features.append('total_rooms')
    
    # 5.3. Phân loại diện tích
    if 'area' in df.columns:
        df['area_category'] = pd.cut(
            df['area'],
            bins=[0, 30, 60, 100, 200, float('inf')],
            labels=['Rất nhỏ', 'Nhỏ', 'Trung bình', 'Lớn', 'Rất lớn']
        ).astype(str)
        new_features.append('area_category')
    
    # 5.4. Mùa trong năm
    if 'month' in df.columns:
        df['season'] = pd.cut(
            df['month'],
            bins=[0, 3, 6, 9, 12],
            labels=['Xuân', 'Hạ', 'Thu', 'Đông']
        ).astype(str)
        new_features.append('season')
    
    # 5.5. Cuối tuần
    if 'day_of_week' in df.columns:
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        new_features.append('is_weekend')
    
    # 5.6. Miền (từ city)
    if 'city' in df.columns:
        southern = ['Hồ Chí Minh', 'Bình Dương', 'Đồng Nai', 'Cần Thơ', 'Bà Rịa Vũng Tàu']
        northern = ['Hà Nội', 'Hải Phòng', 'Quảng Ninh', 'Bắc Ninh']
        
        def get_region(city):
            if pd.isna(city) or city == 'Không xác định':
                return 'Khác'
            if city in southern:
                return 'Miền Nam'
            elif city in northern:
                return 'Miền Bắc'
            else:
                return 'Khác'
        
        df['region'] = df['city'].apply(get_region)
        new_features.append('region')
    
    print(f"  [✓] Đã tạo {len(new_features)} features mới: {new_features}")
    
    return df


# ============================================================
# BƯỚC 6: THỐNG KÊ VÀ BÁO CÁO
# ============================================================
def print_summary(df: pd.DataFrame):
    """In thống kê tổng quan"""
    print("\n" + "="*70)
    print("  [BƯỚC 6] THỐNG KÊ DỮ LIỆU")
    print("="*70)
    
    print(f"\n  Tổng quan:")
    print(f"  - Số giao dịch: {df.shape[0]:,}")
    print(f"  - Số cột: {df.shape[1]}")
    
    if 'city' in df.columns:
        print(f"  - Thành phố: {df['city'].nunique()}")
        print(f"  - Các thành phố: {df['city'].value_counts().to_dict()}")
    
    if 'district' in df.columns:
        print(f"  - Quận/huyện: {df['district'].nunique()}")
    
    if 'area' in df.columns:
        print(f"\n  Diện tích (m²):")
        print(f"  - Min: {df['area'].min():.0f}")
        print(f"  - Mean: {df['area'].mean():.1f}")
        print(f"  - Median: {df['area'].median():.0f}")
        print(f"  - Max: {df['area'].max():.0f}")
    
    if 'price_million' in df.columns:
        print(f"\n  Giá (triệu đồng):")
        print(f"  - Min: {df['price_million'].min():,.0f}")
        print(f"  - Mean: {df['price_million'].mean():,.0f}")
        print(f"  - Median: {df['price_million'].median():,.0f}")
        print(f"  - Max: {df['price_million'].max():,.0f}")
    
    if 'price_per_m2' in df.columns:
        print(f"\n  Giá/m² (triệu đồng):")
        print(f"  - Min: {df['price_per_m2'].min():.4f}")
        print(f"  - Mean: {df['price_per_m2'].mean():.4f}")
        print(f"  - Median: {df['price_per_m2'].median():.4f}")
        print(f"  - Max: {df['price_per_m2'].max():.4f}")
    
    if 'year' in df.columns:
        print(f"\n  Thời gian:")
        year_dist = df['year'].value_counts().sort_index()
        for year, count in year_dist.items():
            print(f"  - Năm {int(year)}: {count} giao dịch")


# ============================================================
# BƯỚC 7: XUẤT FILE
# ============================================================
def export_data(df: pd.DataFrame, output_dir: str):
    """
    Xuất dữ liệu đã xử lý
    """
    print("\n" + "="*70)
    print("  [BƯỚC 7] XUẤT DỮ LIỆU")
    print("="*70)
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Master file (quan trọng nhất)
    master_path = os.path.join(output_dir, 'master_flat_properties_for_ml.csv')
    df.to_csv(master_path, index=False, encoding='utf-8-sig')
    size_kb = os.path.getsize(master_path) / 1024
    print(f"  [✓] master_flat_properties_for_ml.csv")
    print(f"      {df.shape[0]:,} dòng | {df.shape[1]} cột | {size_kb:.0f} KB")
    
    # 2. Fact table sạch
    fact_cols = [
        'property_id', 'time_id', 'location_id', 'type_id', 'trans_id',
        'area', 'price_million', 'price_per_m2',
        'bedrooms_num', 'bathrooms_num'
    ]
    fact_cols = [c for c in fact_cols if c in df.columns]
    
    fact_df = df[fact_cols].copy()
    fact_path = os.path.join(output_dir, 'fact_properties_clean.csv')
    fact_df.to_csv(fact_path, index=False, encoding='utf-8-sig')
    size_kb = os.path.getsize(fact_path) / 1024
    print(f"  [✓] fact_properties_clean.csv ({size_kb:.0f} KB)")
    
    # 3. Xuất lại các dimension đã chuẩn hóa
    dim_exports = {
        'dim_location': ['location_id', 'district', 'city', 'region'],
        'dim_property_type': ['type_id', 'type_name'],
        'dim_transaction_type': ['trans_id', 'trans_name'],
        'dim_time': ['time_id', 'date', 'day', 'month', 'year', 'quarter', 'day_of_week', 'is_weekend']
    }
    
    for dim_name, cols in dim_exports.items():
        available_cols = [c for c in cols if c in df.columns]
        if available_cols:
            dim_df = df[available_cols].drop_duplicates().sort_values(available_cols[0])
            dim_path = os.path.join(output_dir, f'{dim_name}.csv')
            dim_df.to_csv(dim_path, index=False, encoding='utf-8-sig')
            print(f"  [✓] {dim_name}.csv ({dim_df.shape[0]} dòng)")
    
    print(f"\n  → Tất cả file đã lưu tại: {os.path.abspath(output_dir)}")


# ============================================================
# MAIN
# ============================================================
def main():
    """Pipeline chính"""
    
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "   ETL PIPELINE - MERGE 5 FILE DỮ LIỆU BĐS".center(68) + "█")
    print("█" + f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    # BƯỚC 1: Đọc file
    data = load_all_files(CONFIG['raw_data_dir'])
    
    if 'fact' not in data:
        print("\n  [DỪNG] Thiếu file fact_properties.csv!")
        return
    
    # BƯỚC 2: Merge
    df = merge_to_flat_table(data)
    
    if df is None or df.shape[0] == 0:
        print("\n  [DỪNG] Merge thất bại!")
        return
    
    # BƯỚC 3: Làm sạch
    df = clean_merged_data(df)
    
    # BƯỚC 4: Lọc thành phố
    df = filter_by_city(df, CONFIG['target_city'])
    
    if df.shape[0] == 0:
        return
    
    # BƯỚC 5: Thêm features
    df = add_features(df)
    
    # BƯỚC 6: Thống kê
    print_summary(df)
    
    # BƯỚC 7: Xuất
    export_data(df, CONFIG['processed_data_dir'])
    
    # Kết thúc
    print("\n")
    print("█"*70)
    print("█" + " "*68 + "█")
    print("█" + "   HOÀN TẤT!".center(68) + "█")
    print("█" + f"   File ML: master_flat_properties_for_ml.csv".center(68) + "█")
    print("█" + f"   {df.shape[0]:,} dòng | {df.shape[1]} cột".center(68) + "█")
    print("█" + " "*68 + "█")
    print("█"*70)
    
    print(f"\n  Tiếp theo:")
    print(f"  1. Chạy: python src/data_processing/diagnose_data.py")
    print(f"  2. Chạy: python src/ml/02_train_evaluate.py")


if __name__ == "__main__":
    main()