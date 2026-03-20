from __future__ import annotations

import io
import re
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="미샵 샘플 반품 관리 프로그램",
    page_icon="📦",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_FILES = [
    DATA_DIR / "샘플 반납 리스트.zip",
    DATA_DIR / "01-09.xls",
]
BASE_COLUMNS = ["반납일", "반납일표기", "순번", "업체명", "주소", "상품내용", "원본파일"]

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 5.2rem;
        padding-bottom: 3rem;
        max-width: 1450px;
    }
    .main-title {
        font-size: 2rem;
        font-weight: 800;
        margin: 0.45rem 0 0.3rem 0;
    }
    .sub-title {
        color: #555;
        font-size: 1rem;
        margin-bottom: 1.2rem;
    }
    .top-actions {
        margin-bottom: 0.9rem;
    }
    .footer-copy {
        margin-top: 28px;
        padding-top: 16px;
        border-top: 1px solid #e7e7e7;
        text-align: center;
        color: #666;
        font-size: 0.92rem;
    }
    .bottom-guide {
        margin-top: 1rem;
    }
    div[data-testid="stForm"] {
        border: 1px solid #ececec;
        border-radius: 14px;
        padding: 0.8rem 0.9rem 0.2rem 0.9rem;
        background: #fff;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    return re.sub(r"\s+", " ", text)



def parse_date_from_text(text: str) -> Optional[date]:
    if not text:
        return None
    text = str(text).strip()
    patterns = [
        r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})",
        r"(20\d{2})(\d{2})(\d{2})",
        r"(^|[^\d])(\d{1,2})[-./](\d{1,2})([^\d]|$)",
    ]
    current_year = datetime.now().year
    fallback_year = 2026 if current_year >= 2026 else current_year

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        groups = match.groups()
        try:
            if len(groups) >= 3 and groups[0].isdigit() and len(groups[0]) == 4:
                year, month, day = int(groups[0]), int(groups[1]), int(groups[2])
                return date(year, month, day)
            if len(groups) >= 4 and groups[1].isdigit() and groups[2].isdigit():
                month, day = int(groups[1]), int(groups[2])
                return date(fallback_year, month, day)
        except Exception:
            continue
    return None



def safe_date_string(value: object) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.date().strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)



def extract_return_date(df_raw: pd.DataFrame, source_name: str) -> Optional[date]:
    candidates: list[str] = []
    if not df_raw.empty:
        candidates.append(normalize_text(df_raw.iat[0, 0]))
        if df_raw.shape[1] > 1:
            candidates.append(normalize_text(df_raw.iat[0, 1]))
    candidates.append(source_name)
    for candidate in candidates:
        found = parse_date_from_text(candidate)
        if found:
            return found
    return None



def read_excel_bytes(file_bytes: bytes, source_name: str) -> pd.DataFrame:
    raw = pd.read_excel(io.BytesIO(file_bytes), header=None)
    if raw.empty:
        return pd.DataFrame(columns=BASE_COLUMNS)

    return_date = extract_return_date(raw, source_name)
    rows: list[dict[str, object]] = []

    for idx in range(1, len(raw)):
        serial = normalize_text(raw.iat[idx, 0]) if raw.shape[1] > 0 else ""
        vendor = normalize_text(raw.iat[idx, 1]) if raw.shape[1] > 1 else ""
        address = normalize_text(raw.iat[idx, 2]) if raw.shape[1] > 2 else ""
        content = normalize_text(raw.iat[idx, 3]) if raw.shape[1] > 3 else ""

        if vendor == "거래처명" and content == "내용":
            continue
        if not any([serial, vendor, address, content]):
            continue
        if vendor == "":
            continue

        rows.append(
            {
                "반납일": pd.to_datetime(return_date) if return_date else pd.NaT,
                "반납일표기": safe_date_string(return_date),
                "순번": serial,
                "업체명": vendor,
                "주소": address,
                "상품내용": content,
                "원본파일": source_name,
            }
        )

    return pd.DataFrame(rows, columns=BASE_COLUMNS)



def load_from_zip_bytes(file_bytes: bytes) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            member_name = Path(member.filename).name
            if not member_name.lower().endswith((".xls", ".xlsx")):
                continue
            try:
                frame = read_excel_bytes(zf.read(member), member_name)
                if not frame.empty:
                    frames.append(frame)
            except Exception:
                continue
    return frames



def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=BASE_COLUMNS + ["검색용_업체", "검색용_상품"])



def finalize_df(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return empty_df()

    merged = pd.concat(frames, ignore_index=True)
    merged["반납일"] = pd.to_datetime(merged["반납일"], errors="coerce")
    merged = merged.dropna(subset=["반납일", "업체명"])
    merged["업체명"] = merged["업체명"].fillna("").astype(str).str.strip()
    merged["주소"] = merged["주소"].fillna("").astype(str).str.strip()
    merged["상품내용"] = merged["상품내용"].fillna("").astype(str).str.strip()
    merged["원본파일"] = merged["원본파일"].fillna("").astype(str).str.strip()
    merged["반납일표기"] = merged["반납일"].dt.strftime("%Y-%m-%d")
    merged = merged.drop_duplicates(subset=["반납일표기", "업체명", "주소", "상품내용", "원본파일"])
    merged["검색용_업체"] = merged["업체명"].str.lower()
    merged["검색용_상품"] = merged["상품내용"].str.lower()
    merged = merged.sort_values(by=["반납일", "업체명", "순번"], ascending=[False, True, True], kind="stable")
    return merged.reset_index(drop=True)



def load_local_defaults() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    loaded_names: set[str] = set()
    for path in DEFAULT_FILES:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".zip":
                zip_frames = load_from_zip_bytes(path.read_bytes())
                frames.extend(zip_frames)
                loaded_names.update(f.iloc[0]["원본파일"] for f in zip_frames if not f.empty)
            elif path.suffix.lower() in {".xls", ".xlsx"}:
                if path.name in loaded_names:
                    continue
                frame = read_excel_bytes(path.read_bytes(), path.name)
                if not frame.empty:
                    frames.append(frame)
        except Exception:
            continue
    return finalize_df(frames)



def load_uploaded_files(uploaded_files) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for uploaded in uploaded_files or []:
        try:
            file_bytes = uploaded.read()
            lower_name = uploaded.name.lower()
            if lower_name.endswith(".zip"):
                frames.extend(load_from_zip_bytes(file_bytes))
            elif lower_name.endswith((".xls", ".xlsx")):
                frame = read_excel_bytes(file_bytes, uploaded.name)
                if not frame.empty:
                    frames.append(frame)
        except Exception:
            continue
    return finalize_df(frames)



def combine_datasets(base_df: pd.DataFrame, uploaded_df: pd.DataFrame) -> pd.DataFrame:
    if base_df.empty and uploaded_df.empty:
        return empty_df()
    if uploaded_df.empty:
        return base_df.copy()
    if base_df.empty:
        return uploaded_df.copy()
    merged = pd.concat([base_df[BASE_COLUMNS], uploaded_df[BASE_COLUMNS]], ignore_index=True)
    return finalize_df([merged])



def parse_manual_date_range(text: str) -> tuple[Optional[date], Optional[date], Optional[str]]:
    text = (text or "").strip()
    if not text:
        return None, None, None

    parts = re.split(r"\s*~\s*", text)
    if len(parts) == 1:
        one_day = parse_date_from_text(parts[0])
        if one_day is None:
            return None, None, "날짜 형식을 확인해주세요. 예: 2026-03-01~2026-03-20"
        return one_day, one_day, None
    if len(parts) == 2:
        start = parse_date_from_text(parts[0])
        end = parse_date_from_text(parts[1])
        if start is None or end is None:
            return None, None, "날짜 형식을 확인해주세요. 예: 2026-03-01~2026-03-20"
        if start > end:
            start, end = end, start
        return start, end, None
    return None, None, "날짜 형식을 확인해주세요. 예: 2026-03-01~2026-03-20"



def filter_df(
    df: pd.DataFrame,
    mode: str,
    single_date: Optional[date],
    range_dates: tuple[Optional[date], Optional[date]],
    manual_range_text: str,
    vendor_keyword: str,
    product_keyword: str,
) -> tuple[pd.DataFrame, Optional[str]]:
    filtered = df.copy()
    error_msg = None

    if filtered.empty:
        return filtered, error_msg

    # 날짜 필터는 검색 방식에 따라 항상 먼저 적용
    if mode == "하루 검색" and single_date:
        day_ts = pd.Timestamp(single_date)
        filtered = filtered[filtered["반납일"] == day_ts]
    elif mode == "기간 검색":
        start, end = range_dates
        if start and end:
            start_ts = pd.Timestamp(start)
            end_ts = pd.Timestamp(end)
            filtered = filtered[(filtered["반납일"] >= start_ts) & (filtered["반납일"] <= end_ts)]
    elif mode == "수기 입력(~)":
        start, end, error_msg = parse_manual_date_range(manual_range_text)
        if not error_msg and start and end:
            start_ts = pd.Timestamp(start)
            end_ts = pd.Timestamp(end)
            filtered = filtered[(filtered["반납일"] >= start_ts) & (filtered["반납일"] <= end_ts)]

    vendor_keyword = (vendor_keyword or "").strip().lower()
    product_keyword = (product_keyword or "").strip().lower()

    if vendor_keyword:
        filtered = filtered[filtered["검색용_업체"].str.contains(vendor_keyword, na=False)]
    if product_keyword:
        filtered = filtered[filtered["검색용_상품"].str.contains(product_keyword, na=False)]

    return filtered.reset_index(drop=True), error_msg


@st.cache_data(show_spinner=False)
def get_default_dataset() -> pd.DataFrame:
    return load_local_defaults()


if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = 0

current_today = date.today()
current_month_start = current_today.replace(day=1)

st.markdown('<div class="top-actions"></div>', unsafe_allow_html=True)
if st.button("새 작업 / 업로드 초기화"):
    st.session_state.uploader_nonce += 1
    st.rerun()

base_df = get_default_dataset()
uploaded_df = load_uploaded_files(
    st.file_uploader(
        "반납 리스트 업로드 (XLS, XLSX, ZIP / 여러 개 가능)",
        type=["xls", "xlsx", "zip"],
        accept_multiple_files=True,
        key=f"upload_{st.session_state.uploader_nonce}",
        help="새 파일을 업로드하면 기본 등록 데이터와 합쳐서 바로 검색합니다.",
    )
)
all_df = combine_datasets(base_df, uploaded_df)

st.markdown('<div class="main-title">미샵 샘플 반품 관리 프로그램</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">등록된 샘플 반납 리스트에서 반품 내역이 있는지 빠르게 확인하는 용도입니다.</div>',
    unsafe_allow_html=True,
)

search_mode = st.radio(
    "반납일 검색 방식",
    ["기간 검색", "하루 검색", "수기 입력(~)", "전체"],
    horizontal=True,
    index=0,
)

with st.form("search_form"):
    col1, col2, col3, col4 = st.columns([1.35, 1, 1.15, 0.55])
    single_date = None
    range_dates = (None, None)
    manual_range_text = ""

    with col1:
        if search_mode == "기간 검색":
            picked = st.date_input(
                "반납일 범위",
                value=(current_month_start, current_today),
                format="YYYY-MM-DD",
            )
            if isinstance(picked, tuple) and len(picked) == 2:
                range_dates = picked
        elif search_mode == "하루 검색":
            single_date = st.date_input("반납일", value=current_today, format="YYYY-MM-DD")
        elif search_mode == "수기 입력(~)":
            manual_range_text = st.text_input("반납일 수기 입력", placeholder="예: 2026-03-01~2026-03-20")
        else:
            st.text_input("반납일", value="전체", disabled=True)
    with col2:
        vendor_keyword = st.text_input("업체명", placeholder="예: 까르르")
    with col3:
        product_keyword = st.text_input("상품명 / 내용", placeholder="예: 맨투맨, 코트, 슬랙스")
    with col4:
        st.markdown("<div style='height: 1.8rem;'></div>", unsafe_allow_html=True)
        search_clicked = st.form_submit_button("검색", use_container_width=True)

if not search_clicked:
    filtered_df = all_df.iloc[0:0].copy()
    error_msg = None
else:
    filtered_df, error_msg = filter_df(
        all_df,
        search_mode,
        single_date,
        range_dates if isinstance(range_dates, tuple) else tuple(range_dates),
        manual_range_text,
        vendor_keyword,
        product_keyword,
    )

st.markdown("### 검색 결과")
if not search_clicked:
    st.info("검색 조건을 입력한 뒤 검색 버튼을 눌러주세요.")
elif error_msg:
    st.error(error_msg)
elif filtered_df.empty:
    st.warning("등록된 샘플 반납 리스트에서 일치하는 내역이 없습니다.")
else:
    display_df = filtered_df[["반납일표기", "업체명", "주소", "상품내용", "원본파일"]].rename(
        columns={
            "반납일표기": "반납일",
            "상품내용": "액셀 기입 내용",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    st.caption(f"검색 결과 {len(display_df):,}건")

with st.expander("프로그램 안내", expanded=False):
    st.markdown(
        f"""
- 처음 제공해주신 ZIP/XLS 파일 전체를 기본 등록 데이터로 포함했습니다.
- 현재 기본 등록 데이터는 **{len(base_df):,}건**입니다.
- ZIP 안의 엑셀 파일은 모두 읽어서 통합 검색합니다.
- 기간 검색 기본값은 **현재 월 1일 ~ 오늘**입니다.
- 날짜 조건과 업체명/상품명 조건은 **함께 적용**됩니다.
        """
    )

st.markdown(
    '<div class="footer-copy">copyright made by MISHARP COMPANY. MIYAWA. 2026</div>',
    unsafe_allow_html=True,
)
