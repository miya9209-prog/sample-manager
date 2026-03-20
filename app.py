
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
OUTPUT_COLUMNS = ["반납일", "업체명", "주소", "액셀 기입 내용", "원본파일"]


st.markdown(
    """
    <style>
    .block-container {
        padding-top: 4.8rem;
        padding-bottom: 2.6rem;
        max-width: 1450px;
    }
    .main-title {
        font-size: 2rem;
        font-weight: 800;
        margin: 0.2rem 0 0.5rem 0;
    }
    .footer-copy {
        margin-top: 26px;
        padding-top: 16px;
        border-top: 1px solid #e8e8e8;
        text-align: center;
        color: #666;
        font-size: 0.92rem;
    }
    div[data-testid="stForm"] {
        border: 1px solid #ececec;
        border-radius: 14px;
        padding: 0.75rem 0.9rem 0.15rem 0.9rem;
        background: #fff;
    }
    .help-text {
        color: #666;
        font-size: 0.94rem;
        line-height: 1.6;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").replace("\xa0", " ").strip()
    return re.sub(r"\s+", " ", text)


def parse_date_from_text(text: str) -> Optional[date]:
    text = normalize_text(text)
    if not text:
        return None

    patterns = [
        r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})",
        r"(20\d{2})(\d{2})(\d{2})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            y, m, d = map(int, match.groups())
            try:
                return date(y, m, d)
            except ValueError:
                return None
    return None


def parse_date_from_filename(name: str) -> Optional[date]:
    filename = Path(name).name

    m = re.search(r"(20\d{2})-(\d{2})-(\d{2})", filename)
    if m:
        y, mth, d = map(int, m.groups())
        try:
            return date(y, mth, d)
        except ValueError:
            return None

    m = re.search(r"(^|\D)(\d{2})-(\d{2})(?:\D|$)", filename)
    if m:
        mth, d = map(int, m.groups()[1:])
        year_guess = datetime.now().year
        try:
            return date(year_guess, mth, d)
        except ValueError:
            return None
    return None


def read_excel_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame:
    engine = "openpyxl" if filename.lower().endswith(".xlsx") else "xlrd"
    return pd.read_excel(io.BytesIO(file_bytes), header=None, engine=engine)


def convert_one_sheet(raw_df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if raw_df.empty or len(raw_df) < 3:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    return_date = parse_date_from_text(raw_df.iloc[0, 0])
    if return_date is None:
        return_date = parse_date_from_filename(source_name)

    headers = [normalize_text(x) for x in raw_df.iloc[1].tolist()]
    body = raw_df.iloc[2:].copy().reset_index(drop=True)
    body.columns = headers

    vendor_col = next((c for c in ["거래처명", "업체명", "거래처", "업체"] if c in body.columns), None)
    addr_col = next((c for c in ["주소", "위치"] if c in body.columns), None)
    content_col = next((c for c in ["내용", "상품명", "상품", "비고"] if c in body.columns), None)

    if vendor_col is None and len(body.columns) >= 2:
        vendor_col = body.columns[1]
    if addr_col is None and len(body.columns) >= 3:
        addr_col = body.columns[2]
    if content_col is None and len(body.columns) >= 4:
        content_col = body.columns[3]

    out = pd.DataFrame({
        "반납일": [return_date] * len(body),
        "업체명": body[vendor_col].map(normalize_text) if vendor_col in body.columns else "",
        "주소": body[addr_col].map(normalize_text) if addr_col in body.columns else "",
        "액셀 기입 내용": body[content_col].map(normalize_text) if content_col in body.columns else "",
        "원본파일": Path(source_name).name,
    })

    # 의미 없는 빈 행 제거
    out = out[
        (out["업체명"] != "") |
        (out["주소"] != "") |
        (out["액셀 기입 내용"] != "")
    ].copy()

    # 헤더 행 잔존 제거
    bad_vendor_values = {"거래처명", "업체명", "거래처", "업체"}
    out = out[~out["업체명"].isin(bad_vendor_values)].copy()

    return out


def load_zip_bytes(file_bytes: bytes, display_name: str) -> pd.DataFrame:
    frames = []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        for member in zf.namelist():
            lower = member.lower()
            if member.endswith("/") or not (lower.endswith(".xls") or lower.endswith(".xlsx")):
                continue
            try:
                raw_df = read_excel_bytes(zf.read(member), member)
                frames.append(convert_one_sheet(raw_df, member))
            except Exception:
                continue
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def load_single_excel(file_bytes: bytes, display_name: str) -> pd.DataFrame:
    try:
        raw_df = read_excel_bytes(file_bytes, display_name)
        return convert_one_sheet(raw_df, display_name)
    except Exception:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)


def read_path(path: Path) -> bytes:
    with open(path, "rb") as f:
        return f.read()


@st.cache_data(show_spinner=False)
def load_default_data() -> pd.DataFrame:
    frames = []
    seen_files = set()

    for path in DEFAULT_FILES:
        if not path.exists():
            continue

        suffix = path.suffix.lower()
        data = read_path(path)
        if suffix == ".zip":
            df = load_zip_bytes(data, path.name)
            frames.append(df)
            # zip 안의 파일이 이미 다 들어가므로, 개별 파일 중복 제거용 이름 수집
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for member in zf.namelist():
                        if member.lower().endswith((".xls", ".xlsx")):
                            seen_files.add(Path(member).name)
            except Exception:
                pass
        elif suffix in (".xls", ".xlsx"):
            if path.name in seen_files:
                continue
            frames.append(load_single_excel(data, path.name))

    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.concat(frames, ignore_index=True)
    df["반납일"] = pd.to_datetime(df["반납일"], errors="coerce").dt.date
    df = df.dropna(subset=["반납일"]).copy()

    for col in ["업체명", "주소", "액셀 기입 내용", "원본파일"]:
        df[col] = df[col].map(normalize_text)

    df = df.drop_duplicates(subset=["반납일", "업체명", "주소", "액셀 기입 내용", "원본파일"]).copy()
    df = df.sort_values(["반납일", "업체명", "원본파일"], ascending=[False, True, True]).reset_index(drop=True)
    return df


def merge_uploaded(default_df: pd.DataFrame, uploads) -> pd.DataFrame:
    frames = [default_df]
    seen_names = set(default_df["원본파일"].dropna().astype(str).tolist())

    for upload in uploads or []:
        file_name = upload.name
        raw = upload.getvalue()
        suffix = Path(file_name).suffix.lower()

        if suffix == ".zip":
            df = load_zip_bytes(raw, file_name)
        elif suffix in (".xls", ".xlsx"):
            if Path(file_name).name in seen_names:
                continue
            df = load_single_excel(raw, file_name)
        else:
            continue

        if not df.empty:
            frames.append(df)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUTPUT_COLUMNS)
    if merged.empty:
        return merged

    merged["반납일"] = pd.to_datetime(merged["반납일"], errors="coerce").dt.date
    merged = merged.dropna(subset=["반납일"]).copy()
    for col in ["업체명", "주소", "액셀 기입 내용", "원본파일"]:
        merged[col] = merged[col].map(normalize_text)

    merged = merged.drop_duplicates(subset=["반납일", "업체명", "주소", "액셀 기입 내용", "원본파일"]).copy()
    merged = merged.sort_values(["반납일", "업체명", "원본파일"], ascending=[False, True, True]).reset_index(drop=True)
    return merged


def apply_filters(
    df: pd.DataFrame,
    search_type: str,
    start_date: Optional[date],
    end_date: Optional[date],
    single_date: Optional[date],
    manual_text: str,
    vendor_keyword: str,
    product_keyword: str,
) -> tuple[pd.DataFrame, Optional[str]]:
    filtered = df.copy()

    if search_type == "기간 검색":
        if start_date is None or end_date is None:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), "기간 검색 날짜를 확인해주세요."
        if start_date > end_date:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), "시작일이 종료일보다 늦습니다."
        filtered = filtered[(filtered["반납일"] >= start_date) & (filtered["반납일"] <= end_date)].copy()

    elif search_type == "하루 검색":
        if single_date is None:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), "하루 검색 날짜를 선택해주세요."
        filtered = filtered[filtered["반납일"] == single_date].copy()

    elif search_type == "수기 입력(~)":
        manual_text = normalize_text(manual_text)
        m = re.search(r"(20\d{2}-\d{2}-\d{2})\s*~\s*(20\d{2}-\d{2}-\d{2})", manual_text)
        if not m:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), "수기 입력은 2026-03-01~2026-03-20 형식으로 입력해주세요."
        s = parse_date_from_text(m.group(1))
        e = parse_date_from_text(m.group(2))
        if s is None or e is None:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), "수기 입력 날짜를 다시 확인해주세요."
        if s > e:
            return pd.DataFrame(columns=OUTPUT_COLUMNS), "수기 입력 시작일이 종료일보다 늦습니다."
        filtered = filtered[(filtered["반납일"] >= s) & (filtered["반납일"] <= e)].copy()

    vendor_keyword = normalize_text(vendor_keyword)
    if vendor_keyword:
        filtered = filtered[
            filtered["업체명"].astype(str).str.contains(re.escape(vendor_keyword), na=False, regex=True)
        ].copy()

    product_keyword = normalize_text(product_keyword)
    if product_keyword:
        filtered = filtered[
            filtered["액셀 기입 내용"].astype(str).str.contains(re.escape(product_keyword), na=False, regex=True)
        ].copy()

    filtered = filtered[OUTPUT_COLUMNS].reset_index(drop=True)
    return filtered, None


default_df = load_default_data()

if "search_clicked" not in st.session_state:
    st.session_state.search_clicked = False

st.markdown('<div class="main-title">미샵 샘플 반품 관리 프로그램</div>', unsafe_allow_html=True)

today = datetime.now().date()
month_start = today.replace(day=1)

uploads = st.file_uploader(
    "추가 반납 리스트 업로드",
    type=["zip", "xls", "xlsx"],
    accept_multiple_files=True,
    help="처음 제공하신 기본 파일은 이미 등록되어 있습니다. 이후 파일만 추가로 올리면 됩니다.",
)

all_df = merge_uploaded(default_df, uploads)

with st.form("search_form"):
    search_type = st.radio(
        "반납일 검색 방식",
        ["기간 검색", "하루 검색", "수기 입력(~)", "전체"],
        horizontal=True,
        index=0,
    )

    c1, c2, c3, c4 = st.columns([1.25, 0.8, 1.05, 0.5])

    with c1:
        if search_type == "기간 검색":
            date_range = st.date_input(
                "반납일 범위",
                value=(month_start, today),
                format="YYYY-MM-DD",
            )
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
            else:
                start_date, end_date = month_start, today
            single_date = None
            manual_text = ""
        elif search_type == "하루 검색":
            single_date = st.date_input("반납일", value=today, format="YYYY-MM-DD")
            start_date = end_date = None
            manual_text = ""
        elif search_type == "수기 입력(~)":
            manual_text = st.text_input("반납일 범위", value=f"{month_start}~{today}", placeholder="2026-03-01~2026-03-20")
            start_date = end_date = None
            single_date = None
        else:
            st.text_input("반납일 범위", value="전체", disabled=True)
            start_date = end_date = single_date = None
            manual_text = ""

    with c2:
        vendor_keyword = st.text_input("업체명", value="", placeholder="예: 디엠케이")
    with c3:
        product_keyword = st.text_input("상품명 / 내용", value="", placeholder="예: 맨투맨, 코트, 슬랙스")
    with c4:
        st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
        submit = st.form_submit_button("검색", use_container_width=True)

if submit:
    st.session_state.search_clicked = True

if st.session_state.search_clicked:
    filtered_df, error_msg = apply_filters(
        all_df,
        search_type,
        start_date,
        end_date,
        single_date,
        manual_text,
        vendor_keyword,
        product_keyword,
    )

    st.subheader("검색 결과")

    if error_msg:
        st.warning(error_msg)
    elif filtered_df.empty:
        st.info("등록된 샘플 반납 리스트에서 일치하는 내역이 없습니다.")
    else:
        display_df = filtered_df.copy()
        display_df["반납일"] = pd.to_datetime(display_df["반납일"]).dt.strftime("%Y-%m-%d")
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "반납일": st.column_config.TextColumn("반납일", width="small"),
                "업체명": st.column_config.TextColumn("업체명", width="small"),
                "주소": st.column_config.TextColumn("주소", width="medium"),
                "액셀 기입 내용": st.column_config.TextColumn("액셀 기입 내용", width="large"),
                "원본파일": st.column_config.TextColumn("원본파일", width="small"),
            },
        )
        st.caption(f"검색 결과 {len(display_df):,}건")

with st.expander("프로그램 안내"):
    st.markdown(
        f"""
        <div class="help-text">
        기본 등록 데이터는 처음 주신 ZIP과 XLS를 기준으로 불러옵니다.<br>
        현재 기본 등록 건수: <b>{len(default_df):,}건</b><br>
        추가 파일을 업로드하면 기본 데이터에 합쳐서 검색합니다.<br>
        이 프로그램은 등록된 샘플 반납 리스트에 내역이 있는지 확인하는 용도입니다.
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown(
    '<div class="footer-copy">copyright made by MISHARP COMPANY. MIYAWA. 2026</div>',
    unsafe_allow_html=True,
)
