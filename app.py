
import io
import os
import re
import zipfile
from datetime import date, timedelta

import pandas as pd
import streamlit as st

st.set_page_config(page_title="미샵 샘플 반품 관리 프로그램", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 3rem; padding-bottom: 2rem;}
    .stRadio > div {gap: 1rem;}
    .search-card {
        border: 1px solid #e9e9e9;
        border-radius: 16px;
        padding: 16px 16px 8px 16px;
        background: #ffffff;
        margin-bottom: 18px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")

COLUMNS = ["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    return re.sub(r"\s+", " ", text)


def extract_date(raw: pd.DataFrame, source_name: str):
    title = normalize_text(raw.iat[0, 0]) if raw.shape[0] > 0 and raw.shape[1] > 0 else ""
    m = re.search(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})", title)
    if m:
        y, mth, d = map(int, m.groups())
        try:
            return pd.Timestamp(y, mth, d)
        except Exception:
            pass
    # fallback to file name MM-DD.xls => assume current year first, but if that yields future far ahead, use previous year
    base = os.path.basename(source_name)
    m2 = re.search(r"(?<!\d)(\d{1,2})[-./](\d{1,2})(?!\d)", base)
    if m2:
        mth, d = map(int, m2.groups())
        today = date.today()
        for year in [today.year, today.year - 1]:
            try:
                return pd.Timestamp(year, mth, d)
            except Exception:
                continue
    return pd.NaT


def parse_excel_bytes(file_bytes: bytes, source_name: str) -> pd.DataFrame:
    suffix = os.path.splitext(source_name)[1].lower()
    engine = "xlrd" if suffix == ".xls" else "openpyxl"
    raw = pd.read_excel(io.BytesIO(file_bytes), header=None, engine=engine)
    if raw.empty or raw.shape[0] < 3:
        return pd.DataFrame(columns=COLUMNS)

    return_date = extract_date(raw, source_name)

    headers = [normalize_text(v) for v in raw.iloc[1].tolist()]
    headers = [h if h else f"col_{i}" for i, h in enumerate(headers)]
    body = raw.iloc[2:].copy()
    body.columns = headers
    body = body.dropna(how="all")
    if body.empty:
        return pd.DataFrame(columns=COLUMNS)

    col_vendor = None
    col_addr = None
    col_content = None
    for col in body.columns:
        key = normalize_text(col)
        if key in {"거래처명", "업체명", "거래처", "업체"} and col_vendor is None:
            col_vendor = col
        elif key == "주소" and col_addr is None:
            col_addr = col
        elif key in {"내용", "엑셀 기입 내용", "상품명", "상품", "내역"} and col_content is None:
            col_content = col

    # fallback by position for this specific excel layout
    cols = list(body.columns)
    if col_vendor is None and len(cols) >= 2:
        col_vendor = cols[1]
    if col_addr is None and len(cols) >= 3:
        col_addr = cols[2]
    if col_content is None and len(cols) >= 4:
        col_content = cols[3]

    result = pd.DataFrame({
        "반납일": [return_date] * len(body),
        "업체명": body[col_vendor].map(normalize_text) if col_vendor is not None else "",
        "주소": body[col_addr].map(normalize_text) if col_addr is not None else "",
        "엑셀 기입 내용": body[col_content].map(normalize_text) if col_content is not None else "",
        "원본파일": os.path.basename(source_name),
    })
    result = result[(result["업체명"] != "") | (result["엑셀 기입 내용"] != "")]
    result["반납일"] = pd.to_datetime(result["반납일"], errors="coerce")
    result = result.dropna(subset=["반납일"])
    return result.reset_index(drop=True)


def load_from_directory() -> tuple[pd.DataFrame, list[str]]:
    frames, loaded = [], []
    if os.path.isdir(DATA_DIR):
        for file_name in sorted(os.listdir(DATA_DIR)):
            if file_name.lower().endswith((".xls", ".xlsx")):
                path = os.path.join(DATA_DIR, file_name)
                try:
                    with open(path, "rb") as f:
                        df = parse_excel_bytes(f.read(), file_name)
                    if not df.empty:
                        frames.append(df)
                    loaded.append(file_name)
                except Exception:
                    continue
    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=COLUMNS)
    return df, loaded


def load_zip_bytes(file_bytes: bytes) -> tuple[pd.DataFrame, list[str]]:
    frames, loaded = [], []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        for name in zf.namelist():
            if name.lower().endswith((".xls", ".xlsx")):
                try:
                    df = parse_excel_bytes(zf.read(name), name)
                    if not df.empty:
                        frames.append(df)
                    loaded.append(os.path.basename(name))
                except Exception:
                    continue
    if frames:
        return pd.concat(frames, ignore_index=True), loaded
    return pd.DataFrame(columns=COLUMNS), loaded


def load_uploaded(uploaded_files) -> tuple[pd.DataFrame, list[str]]:
    frames, loaded = [], []
    for up in uploaded_files or []:
        lower = up.name.lower()
        data = up.getvalue()
        try:
            if lower.endswith(".zip"):
                df, names = load_zip_bytes(data)
                if not df.empty:
                    frames.append(df)
                loaded.extend(names)
            elif lower.endswith((".xls", ".xlsx")):
                df = parse_excel_bytes(data, up.name)
                if not df.empty:
                    frames.append(df)
                loaded.append(up.name)
        except Exception:
            continue
    if frames:
        return pd.concat(frames, ignore_index=True), loaded
    return pd.DataFrame(columns=COLUMNS), loaded


@st.cache_data(show_spinner=False)
def get_base_data():
    return load_from_directory()


def get_all_data(uploaded_files):
    base_df, base_files = get_base_data()
    up_df, up_files = load_uploaded(uploaded_files)
    if not up_df.empty:
        all_df = pd.concat([base_df, up_df], ignore_index=True)
    else:
        all_df = base_df.copy()
    if not all_df.empty:
        all_df["반납일"] = pd.to_datetime(all_df["반납일"], errors="coerce")
        all_df = all_df.dropna(subset=["반납일"]).drop_duplicates().reset_index(drop=True)
    return all_df, sorted(set(base_files + up_files))


def get_default_range(df: pd.DataFrame):
    today = pd.Timestamp(date.today())
    if df.empty:
        return today.replace(day=1).date(), today.date(), "현재 월 기준"
    df = df.copy()
    df["반납일"] = pd.to_datetime(df["반납일"], errors="coerce")
    df = df.dropna(subset=["반납일"])
    current_month = df[(df["반납일"].dt.year == today.year) & (df["반납일"].dt.month == today.month)]
    if not current_month.empty:
        return today.replace(day=1).date(), today.date(), "현재 월 기준"
    latest = df["반납일"].max()
    start = latest.replace(day=1).date()
    end = latest.date()
    return start, end, f"데이터 최신 월 기준 ({latest.strftime('%Y-%m')})"


def filter_df(df, search_mode, start_date=None, end_date=None, single_date=None, manual_text="", vendor_keyword="", product_keyword=""):
    filtered = df.copy()
    if filtered.empty:
        return filtered, None

    filtered["반납일"] = pd.to_datetime(filtered["반납일"], errors="coerce")
    filtered = filtered.dropna(subset=["반납일"])

    if search_mode == "기간 검색" and start_date and end_date:
        s = pd.Timestamp(start_date)
        e = pd.Timestamp(end_date)
        filtered = filtered[(filtered["반납일"] >= s) & (filtered["반납일"] <= e)]
    elif search_mode == "하루 검색" and single_date:
        d = pd.Timestamp(single_date)
        filtered = filtered[filtered["반납일"] == d]
    elif search_mode == "수기 입력(~)":
        t = normalize_text(manual_text)
        if t:
            m = re.match(r"(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})", t)
            if not m:
                return filtered.iloc[0:0], "수기 입력은 2025-03-01~2025-03-20 형식으로 입력해주세요."
            s = pd.Timestamp(m.group(1))
            e = pd.Timestamp(m.group(2))
            filtered = filtered[(filtered["반납일"] >= s) & (filtered["반납일"] <= e)]

    vendor_keyword = normalize_text(vendor_keyword)
    product_keyword = normalize_text(product_keyword)

    if vendor_keyword:
        vendor_series = filtered["업체명"].astype(str).map(normalize_text)
        filtered = filtered[vendor_series.str.contains(vendor_keyword, na=False, regex=False)]

    if product_keyword:
        product_series = filtered["엑셀 기입 내용"].astype(str).map(normalize_text)
        filtered = filtered[product_series.str.contains(product_keyword, na=False, regex=False)]

    filtered = filtered.sort_values(["반납일", "업체명", "원본파일"], ascending=[False, True, True]).reset_index(drop=True)
    return filtered, None


if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = 0
if "search_triggered" not in st.session_state:
    st.session_state.search_triggered = False

st.title("미샵 샘플 반품 관리 프로그램")

left_top, _ = st.columns([1, 4])
with left_top:
    if st.button("업로드 초기화", use_container_width=True):
        st.session_state.uploader_nonce += 1
        st.session_state.search_triggered = False
        st.rerun()

uploaded_files = st.file_uploader(
    "추가 파일 업로드 (ZIP / XLS / XLSX)",
    type=["zip", "xls", "xlsx"],
    accept_multiple_files=True,
    key=f"uploader_{st.session_state.uploader_nonce}",
)

all_df, loaded_files = get_all_data(uploaded_files)
default_start, default_end, default_label = get_default_range(all_df)

st.markdown('<div class="search-card">', unsafe_allow_html=True)
st.caption("반납일 검색 방식")
search_mode = st.radio(
    "반납일 검색 방식",
    ["기간 검색", "하루 검색", "수기 입력(~)", "전체"],
    index=0,
    horizontal=True,
    label_visibility="collapsed",
)

col1, col2, col3, col4 = st.columns([1.8, 1.2, 1.5, 0.7])
with col1:
    if search_mode == "기간 검색":
        date_range = st.date_input("반납일 범위", value=(default_start, default_end), format="YYYY-MM-DD")
        if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
            start_date, end_date = date_range[0], date_range[1]
        else:
            start_date, end_date = default_start, default_end
        single_date = None
        manual_text = ""
    elif search_mode == "하루 검색":
        single_date = st.date_input("반납일", value=default_end, format="YYYY-MM-DD")
        start_date = end_date = None
        manual_text = ""
    elif search_mode == "수기 입력(~)":
        manual_text = st.text_input("반납일 범위", value=f"{default_start.isoformat()}~{default_end.isoformat()}", placeholder="2025-03-01~2025-03-20")
        start_date = end_date = single_date = None
    else:
        st.text_input("반납일", value="전체", disabled=True)
        start_date = end_date = single_date = None
        manual_text = ""
with col2:
    vendor_keyword = st.text_input("업체명", placeholder="예: 디엠케이")
with col3:
    product_keyword = st.text_input("상품명 / 내용", placeholder="예: 맨투맨, 코트, 슬랙스")
with col4:
    st.write("")
    st.write("")
    search_clicked = st.button("검색", use_container_width=True)
st.markdown("</div>", unsafe_allow_html=True)

if search_clicked:
    st.session_state.search_triggered = True

st.subheader("검색 결과")
if st.session_state.search_triggered:
    filtered_df, error_msg = filter_df(all_df, search_mode, start_date, end_date, single_date, manual_text, vendor_keyword, product_keyword)
    if error_msg:
        st.warning(error_msg)
    elif filtered_df.empty:
        st.info("일치하는 내역이 없습니다. 현재 선택한 날짜 범위와 실제 등록 데이터의 연도를 함께 확인해주세요.")
        if not all_df.empty:
            st.caption(
                f"등록 데이터 기간: {all_df['반납일'].min().strftime('%Y-%m-%d')} ~ {all_df['반납일'].max().strftime('%Y-%m-%d')}"
            )
    else:
        display_df = filtered_df.copy()
        display_df["반납일"] = display_df["반납일"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.caption(f"검색 결과 {len(display_df):,}건")
else:
    st.caption("검색 조건을 입력한 뒤 검색 버튼을 눌러주세요.")

with st.expander("프로그램 안내"):
    st.write("- 기본 데이터는 ZIP 내부 전체 엑셀 파일을 풀어서 data 폴더에 포함했습니다.")
    st.write("- 업체명만 입력해도 검색되고, 상품명/내용만 입력해도 검색됩니다.")
    st.write(f"- 기간 검색 기본값은 {default_label}으로 자동 설정됩니다.")
    if not all_df.empty:
        st.write(f"- 등록 데이터 기간: {all_df['반납일'].min().strftime('%Y-%m-%d')} ~ {all_df['반납일'].max().strftime('%Y-%m-%d')}")
    st.write(f"- 현재 등록 파일 수: {len(loaded_files)}개")
    st.write(f"- 현재 등록 건수: {len(all_df):,}건")

st.markdown("<div style='margin-top:24px; color:#666; font-size:12px;'>copyright made by MISHARP COMPANY. MIYAWA. 2026</div>", unsafe_allow_html=True)
