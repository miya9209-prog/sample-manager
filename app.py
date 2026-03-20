
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
BASE_COLUMNS = ["반납일", "업체명", "주소", "액셀 기입 내용", "원본파일"]

st.markdown(
    """
    <style>
    .block-container {
        padding-top: 5.7rem;
        padding-bottom: 2.8rem;
        max-width: 1450px;
    }
    .main-title {
        font-size: 2rem;
        font-weight: 800;
        margin: 0.35rem 0 0.2rem 0;
    }
    .sub-title {
        color: #555;
        font-size: 0.98rem;
        margin-bottom: 1rem;
    }
    .footer-copy {
        margin-top: 28px;
        padding-top: 16px;
        border-top: 1px solid #e7e7e7;
        text-align: center;
        color: #666;
        font-size: 0.92rem;
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
    text = normalize_text(text)
    if not text:
        return None

    patterns = [
        r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})",
        r"(20\d{2})(\d{2})(\d{2})",
        r"(?<!\d)(\d{1,2})[-./](\d{1,2})(?!\d)",
    ]
    fallback_year = datetime.now().year

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            if len(match.groups()) == 3 and len(match.group(1)) == 4:
                return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            if len(match.groups()) == 3:
                return date(fallback_year, int(match.group(1)), int(match.group(2)))
        except Exception:
            continue
    return None


def detect_header_row(raw: pd.DataFrame) -> int:
    search_rows = min(len(raw), 8)
    for i in range(search_rows):
        row_values = [normalize_text(v) for v in raw.iloc[i].tolist()]
        joined = " ".join(row_values)
        if "거래처명" in joined or ("주소" in joined and "내용" in joined):
            return i
    return 1 if len(raw) > 1 else 0


def extract_return_date(raw: pd.DataFrame, source_name: str) -> Optional[date]:
    candidates: list[str] = []

    for r in range(min(len(raw), 3)):
        for c in range(min(raw.shape[1], 4)):
            candidates.append(normalize_text(raw.iat[r, c]))

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
    header_row = detect_header_row(raw)
    data_start = min(header_row + 1, len(raw))
    rows: list[dict[str, object]] = []

    for idx in range(data_start, len(raw)):
        serial = normalize_text(raw.iat[idx, 0]) if raw.shape[1] > 0 else ""
        vendor = normalize_text(raw.iat[idx, 1]) if raw.shape[1] > 1 else ""
        address = normalize_text(raw.iat[idx, 2]) if raw.shape[1] > 2 else ""
        content = normalize_text(raw.iat[idx, 3]) if raw.shape[1] > 3 else ""

        if vendor in {"", "거래처명"}:
            continue
        if content == "내용" and address == "주소":
            continue
        if vendor.lower().startswith("합계"):
            continue
        if not any([vendor, address, content]):
            continue

        rows.append(
            {
                "반납일": pd.to_datetime(return_date) if return_date else pd.NaT,
                "업체명": vendor.strip(),
                "주소": address.strip(),
                "액셀 기입 내용": content.strip(),
                "원본파일": source_name,
            }
        )

    return pd.DataFrame(rows, columns=BASE_COLUMNS)


def load_from_zip_bytes(file_bytes: bytes) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        members = [m for m in zf.infolist() if not m.is_dir()]
        for member in members:
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


def finalize_df(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=BASE_COLUMNS + ["검색용_업체명", "검색용_내용"])

    merged = pd.concat(frames, ignore_index=True)
    merged["반납일"] = pd.to_datetime(merged["반납일"], errors="coerce")
    merged = merged.dropna(subset=["반납일"])
    for col in ["업체명", "주소", "액셀 기입 내용", "원본파일"]:
        merged[col] = merged[col].fillna("").astype(str).str.strip()
    merged = merged[merged["업체명"] != ""]
    merged = merged.drop_duplicates(subset=BASE_COLUMNS)
    merged["검색용_업체명"] = merged["업체명"].str.lower().str.replace(" ", "", regex=False)
    merged["검색용_내용"] = merged["액셀 기입 내용"].str.lower().str.replace(" ", "", regex=False)
    merged = merged.sort_values(by=["반납일", "업체명", "원본파일"], ascending=[False, True, True], kind="stable")
    return merged.reset_index(drop=True)


def load_local_defaults() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    loaded_excel_names: set[str] = set()

    for path in DEFAULT_FILES:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".zip":
                zip_frames = load_from_zip_bytes(path.read_bytes())
                for frame in zip_frames:
                    frames.append(frame)
                    if not frame.empty:
                        loaded_excel_names.update(frame["원본파일"].dropna().astype(str).tolist())
            elif path.suffix.lower() in {".xls", ".xlsx"}:
                if path.name in loaded_excel_names:
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
        return finalize_df([])
    if base_df.empty:
        return uploaded_df.copy()
    if uploaded_df.empty:
        return base_df.copy()
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
    if filtered.empty:
        return filtered, None

    error_msg = None
    if mode == "하루 검색" and single_date:
        target = pd.Timestamp(single_date).normalize()
        filtered = filtered[filtered["반납일"].dt.normalize() == target]
    elif mode == "기간 검색":
        start, end = range_dates
        if start and end:
            start_ts = pd.Timestamp(start).normalize()
            end_ts = pd.Timestamp(end).normalize()
            filtered = filtered[(filtered["반납일"].dt.normalize() >= start_ts) & (filtered["반납일"].dt.normalize() <= end_ts)]
    elif mode == "수기 입력(~)":
        start, end, error_msg = parse_manual_date_range(manual_range_text)
        if not error_msg and start and end:
            start_ts = pd.Timestamp(start).normalize()
            end_ts = pd.Timestamp(end).normalize()
            filtered = filtered[(filtered["반납일"].dt.normalize() >= start_ts) & (filtered["반납일"].dt.normalize() <= end_ts)]

    vendor_keyword = (vendor_keyword or "").strip().lower().replace(" ", "")
    product_keyword = (product_keyword or "").strip().lower().replace(" ", "")

    if vendor_keyword:
        filtered = filtered[filtered["검색용_업체명"].str.contains(re.escape(vendor_keyword), na=False, regex=True)]
    if product_keyword:
        filtered = filtered[filtered["검색용_내용"].str.contains(re.escape(product_keyword), na=False, regex=True)]

    return filtered.reset_index(drop=True), error_msg


@st.cache_data(show_spinner=False)
def get_default_dataset() -> pd.DataFrame:
    return load_local_defaults()


if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = 0

today = date.today()
month_start = today.replace(day=1)

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
    '<div class="sub-title">등록된 샘플 반납 리스트에서 반품 내역이 있는지 확인하는 프로그램입니다.</div>',
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
            picked = st.date_input("반납일 범위", value=(month_start, today), format="YYYY-MM-DD")
            if isinstance(picked, tuple) and len(picked) == 2:
                range_dates = picked
        elif search_mode == "하루 검색":
            single_date = st.date_input("반납일", value=today, format="YYYY-MM-DD")
        elif search_mode == "수기 입력(~)":
            manual_range_text = st.text_input("반납일 수기 입력", placeholder="예: 2026-03-01~2026-03-20")
        else:
            st.text_input("반납일", value="전체", disabled=True)

    with col2:
        vendor_keyword = st.text_input("업체명", placeholder="예: 디엠케이, 까르르")
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
        range_dates,
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
    display_df = filtered_df[["반납일", "업체명", "주소", "액셀 기입 내용", "원본파일"]].copy()
    display_df["반납일"] = pd.to_datetime(display_df["반납일"]).dt.strftime("%Y-%m-%d")
    st.dataframe(display_df, use_container_width=True, hide_index=True)
    st.caption(f"검색 결과 {len(display_df):,}건")

with st.expander("프로그램 안내", expanded=False):
    st.markdown(
        f"""
- 처음 주신 ZIP/XLS 파일을 기본 등록 데이터로 포함했습니다.
- 현재 기본 등록 건수: **{len(base_df):,}건**
- ZIP 안의 엑셀 파일은 모두 읽어서 통합 검색합니다.
- 기간 검색 기본값은 **현재 월 1일 ~ 오늘**입니다.
- 업체명만 입력해도 검색되고, 상품명만 입력해도 검색됩니다.
        """
    )

st.markdown(
    '<div class="footer-copy">copyright made by MISHARP COMPANY. MIYAWA. 2026</div>',
    unsafe_allow_html=True,
)
