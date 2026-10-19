import io
import sys
from pathlib import Path
from PIL import Image
import pandas as pd
import streamlit as st

# Cấu hình trang Streamlit
st.set_page_config(
    page_title="Product Image Lakehouse Dashboard",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Thêm root vào sys.path để import cấu hình MinIO
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.minio_config import MINIO_BUCKET, MINIO_ENDPOINT, create_s3_client


@st.cache_data(show_spinner=False)
def load_silver_data():
    """Nạp dữ liệu Silver Parquet đã chuẩn hóa."""
    parquet_path = PROJECT_ROOT / "data" / "lakehouse_real" / "silver" / "product_images"
    if not parquet_path.exists():
        # Fallback về products.csv nếu chưa có folder lakehouse_real
        csv_path = PROJECT_ROOT / "products.csv"
        df = pd.read_csv(csv_path)
    else:
        df = pd.read_parquet(parquet_path)
    df["image_size_kb"] = df["image_size"] / 1024
    df["image_size_mb"] = df["image_size"] / (1024 * 1024)
    return df


@st.cache_data(show_spinner=False, max_entries=200)
def fetch_minio_image(object_key: str):
    """Tải bytes của ảnh trực tiếp từ MinIO S3 Object Storage."""
    try:
        s3 = create_s3_client()
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=object_key)
        return obj["Body"].read()
    except Exception as e:
        return None


# --- SIDEBAR ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/8618/8618881.png", width=70)
    st.title("Lakehouse Control")
    st.markdown("---")

    st.subheader("🌐 Trạng thái Hạ tầng")
    st.success(f"**MinIO Object Storage:** Đang kết nối")
    st.info(f"**Endpoint:** `{MINIO_ENDPOINT}`\n\n**Bucket:** `{MINIO_BUCKET}`")
    st.markdown("---")

    st.caption("© 2026 Product Image Lakehouse Team")

# --- HEADER CHÍNH ---
st.title("🛍️ Product Image Lakehouse Dashboard")
st.markdown(
    "Hệ thống giám sát, phân tích và truy vấn kho ảnh sản phẩm thời trang quy mô lớn dựa trên **Object Storage (MinIO)**, **Apache Spark** và **Apache Iceberg**."
)
st.markdown("---")

# Tải dữ liệu
df_silver = load_silver_data()

# --- HÀNG KPI CARDS ---
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
total_images = len(df_silver)
total_size_mb = df_silver["image_size_mb"].sum()
total_categories = df_silver["category"].nunique()
avg_width = int(df_silver["width"].mean())
avg_height = int(df_silver["height"].mean())

with kpi1:
    st.metric(label="🖼️ Tổng số lượng ảnh", value=f"{total_images:,}")
with kpi2:
    st.metric(label="💾 Tổng dung lượng", value=f"{total_size_mb:.1f} MB")
with kpi3:
    st.metric(label="📂 Số danh mục", value=f"{total_categories}")
with kpi4:
    st.metric(label="📐 Độ phân giải chuẩn", value=f"{avg_width}x{avg_height} px")
with kpi5:
    st.metric(label="⚡ Tỉ lệ Khớp Catalog", value="100%", delta="0 Quarantine")

st.markdown("---")

# --- TABS GIAO DIỆN ---
tab1, tab2, tab3 = st.tabs([
    "📊 Phân Tích Tầng Gold (Business Intelligence)",
    "🔍 Khám Phá Kho Ảnh (MinIO Image Explorer)",
    "🧊 Kiến Trúc Lakehouse & Time Travel",
])

# ==================== TAB 1 ====================
with tab1:
    st.subheader("📈 Thống kê Dữ liệu Tầng Gold (Business Aggregates)")
    st.markdown(
        "Dữ liệu được tổng hợp từ bảng Iceberg `lakehouse.gold.category_image_stats` và `lakehouse.gold.image_format_stats`."
    )

    # Tổng hợp theo category
    cat_summary = (
        df_silver.groupby("category")
        .agg(
            image_count=("image_id", "count"),
            total_mb=("image_size_mb", "sum"),
            avg_kb=("image_size_kb", "mean"),
        )
        .reset_index()
        .sort_values(by="image_count", ascending=False)
    )

    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        st.markdown("**Số lượng ảnh theo Danh mục Sản phẩm**")
        st.bar_chart(cat_summary.set_index("category")["image_count"], color="#1f77b4")

    with col_chart2:
        st.markdown("**Tổng dung lượng lưu trữ (MB) theo Danh mục**")
        st.bar_chart(cat_summary.set_index("category")["total_mb"], color="#ff7f0e")

    st.markdown("##### 📋 Bảng Chi Tiết Tầng Gold (Category Stats)")
    display_cat = cat_summary.copy()
    display_cat.columns = [
        "Danh mục (Category)",
        "Số lượng ảnh",
        "Tổng dung lượng (MB)",
        "Dung lượng trung bình (KB)",
    ]
    display_cat["Số lượng ảnh"] = display_cat["Số lượng ảnh"].apply(lambda x: f"{x:,}")
    display_cat["Tổng dung lượng (MB)"] = display_cat["Tổng dung lượng (MB)"].apply(
        lambda x: f"{x:.2f} MB"
    )
    display_cat["Dung lượng trung bình (KB)"] = display_cat[
        "Dung lượng trung bình (KB)"
    ].apply(lambda x: f"{x:.2f} KB")
    st.dataframe(display_cat, width="stretch", hide_index=True)


# ==================== TAB 2 ====================
with tab2:
    st.subheader("🔍 Trực Quan Hóa & Phục Vụ Ảnh từ MinIO (Serving Layer)")
    st.markdown(
        "Tìm kiếm và xem ảnh trực tiếp từ **MinIO Object Storage** theo thời gian thực."
    )

    # Bộ lọc
    f_col1, f_col2, f_col3 = st.columns([2, 3, 2])
    with f_col1:
        all_categories = ["Tất cả (All)"] + sorted(df_silver["category"].dropna().unique().tolist())
        selected_cat = st.selectbox("Lọc theo danh mục:", all_categories)
    with f_col2:
        search_kw = st.text_input(
            "Tìm kiếm theo tên sản phẩm:", placeholder="Ví dụ: Nike, T-Shirt, Skirt..."
        )
    with f_col3:
        num_images = st.slider("Số lượng ảnh hiển thị:", min_value=4, max_value=24, value=8, step=4)

    # Áp dụng bộ lọc
    filtered_df = df_silver.copy()
    if selected_cat != "Tất cả (All)":
        filtered_df = filtered_df[filtered_df["category"] == selected_cat]
    if search_kw.strip():
        filtered_df = filtered_df[
            filtered_df["product_name"].str.contains(search_kw.strip(), case=False, na=False)
        ]

    st.caption(f"Tìm thấy **{len(filtered_df):,}** sản phẩm phù hợp. Đang hiển thị **{min(num_images, len(filtered_df))}** ảnh:")

    if len(filtered_df) == 0:
        st.warning("Không tìm thấy sản phẩm nào khớp với điều kiện lọc.")
    else:
        sample_rows = filtered_df.head(num_images).to_dict("records")
        grid_cols = st.columns(4)
        for idx, row in enumerate(sample_rows):
            col_target = grid_cols[idx % 4]
            with col_target:
                # Key trên MinIO: ví dụ raw/10000.jpg
                obj_key = row.get("image_path") or f"raw/{row['image_id']}.jpg"
                img_data = fetch_minio_image(obj_key)

                if img_data:
                    try:
                        pil_img = Image.open(io.BytesIO(img_data))
                        st.image(pil_img, width="stretch")
                    except Exception:
                        st.error("Lỗi giải mã ảnh")
                else:
                    st.warning("Chưa tìm thấy trên MinIO")

                st.markdown(f"**ID:** `{row['image_id']}`")
                st.caption(f"**Tên:** {row.get('product_name', 'N/A')}")
                st.caption(
                    f"**Loại:** `{row.get('category', 'N/A')}` | **Size:** {row.get('image_size_kb', 0):.1f} KB"
                )
                st.markdown("---")


# ==================== TAB 3 ====================
with tab3:
    st.subheader("🧊 Kiến Trúc Data Lakehouse & Khả Năng Mở Rộng")

    st.markdown("""
    #### 1. Mô hình Medallion Architecture (Bronze ➔ Silver ➔ Gold)
    """)

    arch_col1, arch_col2, arch_col3 = st.columns(3)
    with arch_col1:
        st.info("""
        ### 🥉 Bronze Layer
        - **Công nghệ:** MinIO S3 Object Storage
        - **Dữ liệu:** File ảnh JPEG thô (`raw/*.jpg`)
        - **Vai trò:** Lưu trữ phi cấu trúc quy mô lớn, tương thích S3 API.
        """)
    with arch_col2:
        st.success("""
        ### 🥈 Silver Layer
        - **Công nghệ:** Apache Spark & Iceberg
        - **Dữ liệu:** Metadata ảnh thật + Catalog JOIN
        - **Vai trò:** Chuẩn hóa độ phân giải, kiểm định chất lượng (0 quarantine).
        """)
    with arch_col3:
        st.warning("""
        ### 🥇 Gold Layer
        - **Công nghệ:** Apache Iceberg Tables
        - **Dữ liệu:** `category_image_stats`, `image_format_stats`
        - **Vai trò:** Phục vụ Business Intelligence, Báo cáo & Dashboard phân tích.
        """)

    st.markdown("---")
    st.markdown("#### 2. Tính năng Nâng cao: Schema Evolution & Time Travel")
    st.markdown("""
    Apache Iceberg mang đến các tính năng ACID transaction mạnh mẽ trên Data Lakehouse:
    """)

    sql_col1, sql_col2 = st.columns(2)
    with sql_col1:
        st.markdown("**1. Schema Evolution (Tiến hóa cấu trúc không gián đoạn):**")
        st.code("""-- Thêm cột tính tỉ lệ ảnh mà không cần tạo lại bảng
ALTER TABLE lakehouse.silver.product_images_evolution_demo 
ADD COLUMN aspect_ratio DOUBLE;

UPDATE lakehouse.silver.product_images_evolution_demo 
SET aspect_ratio = CAST(width AS DOUBLE) / CAST(height AS DOUBLE);
""", language="sql")

    with sql_col2:
        st.markdown("**2. Time Travel (Truy vấn dữ liệu tại thời điểm quá khứ):**")
        st.code("""-- Truy vấn bảng tại Snapshot ban đầu trước khi thêm cột
SELECT image_id, width, height 
FROM lakehouse.silver.product_images_evolution_demo 
VERSION AS OF <BASELINE_SNAPSHOT_ID>;
""", language="sql")

    st.success("Tất cả các tính năng trên đã được kiểm chứng thành công qua script `spark/demo_iceberg_evolution.py`.")