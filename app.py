
import io
import os
import re
import zipfile
from datetime import date

import pandas as pd
import streamlit as st

st.set_page_config(page_title="미샵 샘플 반품 관리 프로그램", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 2.4rem; padding-bottom: 2rem;}
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

DEFAULT_DATA_DIR = "data"
DEFAULT_ZIP = "샘플 반납 리스트.zip"
DEFAULT_XLS = "01-09.xls"


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def extract_date(title: str, source_name: str) -> pd.Timestamp | pd.NaT:
    title = normalize_text(title)
    source_name = normalize_text(source_name)

    m = re.search(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})", title)
    if not m:
        m = re.search(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})", source_name)
    if m:
        y, mth, d = map(int, m.groups())
        try:
            return pd.Timestamp(y, mth, d)
        except Exception:
            return pd.NaT

    # fallback for names like 03-05.xls -> assume current year
    base = os.path.basename(source_name)
    m2 = re.search(r"(?<!\d)(\d{1,2})[-./](\d{1,2})(?!\d)", base)
    if m2:
        mth, d = map(int, m2.groups())
        try:
            return pd.Timestamp(date.today().year, mth, d)
        except Exception:
            return pd.NaT
    return pd.NaT


def parse_excel_bytes(file_bytes: bytes, source_name: str) -> pd.DataFrame:
    suffix = os.path.splitext(source_name)[1].lower()
    engine = "xlrd" if suffix == ".xls" else "openpyxl"
    raw = pd.read_excel(io.BytesIO(file_bytes), header=None, engine=engine)
    if raw.empty:
        return pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"])

    title = normalize_text(raw.iat[0, 0]) if raw.shape[0] >= 1 and raw.shape[1] >= 1 else ""
    return_date = extract_date(title, source_name)

    if raw.shape[0] < 2:
        return pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"])

    headers = [normalize_text(v) for v in raw.iloc[1].tolist()]
    headers = [h if h else f"col_{i}" for i, h in enumerate(headers)]

    body = raw.iloc[2:].copy()
    body.columns = headers
    body = body.dropna(how="all")
    if body.empty:
        return pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"])

    rename_map: dict[str, str] = {}
    for col in body.columns:
        text = normalize_text(col)
        if text in {"거래처명", "업체명", "거래처", "업체"}:
            rename_map[col] = "업체명"
        elif text == "주소":
            rename_map[col] = "주소"
        elif text in {"내용", "엑셀 기입 내용", "상품명", "상품", "내역"}:
            rename_map[col] = "엑셀 기입 내용"
        elif text in {"번호", "순번", "no", "No", "NO", "Unnamed: 0", "col_0"}:
            rename_map[col] = "번호"

    body = body.rename(columns=rename_map)

    if "업체명" not in body.columns:
        if len(body.columns) >= 2:
            body = body.rename(columns={body.columns[1]: "업체명"})
        else:
            body["업체명"] = ""
    if "주소" not in body.columns:
        if len(body.columns) >= 3:
            body = body.rename(columns={body.columns[2]: "주소"})
        else:
            body["주소"] = ""
    if "엑셀 기입 내용" not in body.columns:
        if len(body.columns) >= 4:
            body = body.rename(columns={body.columns[3]: "엑셀 기입 내용"})
        else:
            body["엑셀 기입 내용"] = ""

    result = pd.DataFrame()
    result["반납일"] = return_date
    result["업체명"] = body["업체명"].map(normalize_text)
    result["주소"] = body["주소"].map(normalize_text)
    result["엑셀 기입 내용"] = body["엑셀 기입 내용"].map(normalize_text)
    result["원본파일"] = os.path.basename(source_name)
    result = result[(result["업체명"] != "") | (result["엑셀 기입 내용"] != "")]
    return result.reset_index(drop=True)


def load_from_zip_bytes(zip_bytes: bytes) -> tuple[pd.DataFrame, list[str]]:
    dfs, loaded = [], []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if name.lower().endswith((".xls", ".xlsx")):
                try:
                    dfs.append(parse_excel_bytes(zf.read(name), name))
                    loaded.append(os.path.basename(name))
                except Exception:
                    continue
    if dfs:
        return pd.concat(dfs, ignore_index=True), loaded
    return pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]), loaded


def load_single_excel_bytes(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    return parse_excel_bytes(file_bytes, file_name)


@st.cache_data(show_spinner=False)
def load_default_data() -> tuple[pd.DataFrame, list[str]]:
    app_dir = os.path.dirname(os.path.abspath(__file__))
    frames: list[pd.DataFrame] = []
    loaded_files: list[str] = []

    data_dir = os.path.join(app_dir, DEFAULT_DATA_DIR)
    if os.path.isdir(data_dir):
        for file_name in sorted(os.listdir(data_dir)):
            if file_name.lower().endswith((".xls", ".xlsx")):
                path = os.path.join(data_dir, file_name)
                try:
                    with open(path, "rb") as f:
                        df_xls = load_single_excel_bytes(f.read(), file_name)
                    if not df_xls.empty:
                        frames.append(df_xls)
                    loaded_files.append(file_name)
                except Exception:
                    continue
    else:
        zip_path = os.path.join(app_dir, DEFAULT_ZIP)
        if os.path.exists(zip_path):
            with open(zip_path, "rb") as f:
                df_zip, loaded = load_from_zip_bytes(f.read())
                if not df_zip.empty:
                    frames.append(df_zip)
                loaded_files.extend(loaded)
        xls_path = os.path.join(app_dir, DEFAULT_XLS)
        if os.path.exists(xls_path):
            with open(xls_path, "rb") as f:
                df_xls = load_single_excel_bytes(f.read(), DEFAULT_XLS)
                if not df_xls.empty:
                    frames.append(df_xls)
                    loaded_files.append(DEFAULT_XLS)

    if frames:
        df = pd.concat(frames, ignore_index=True)
        df["반납일"] = pd.to_datetime(df["반납일"], errors="coerce")
        df = df.dropna(subset=["반납일"]).drop_duplicates(
            subset=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]
        ).reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"])
    return df, sorted(set(loaded_files))


def load_uploaded_files(uploaded_files) -> tuple[pd.DataFrame, list[str]]:
    if not uploaded_files:
        return pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]), []

    frames: list[pd.DataFrame] = []
    loaded_files: list[str] = []
    for up in uploaded_files:
        file_name = up.name
        file_bytes = up.getvalue()
        lower = file_name.lower()
        try:
            if lower.endswith(".zip"):
                df_zip, loaded = load_from_zip_bytes(file_bytes)
                if not df_zip.empty:
                    frames.append(df_zip)
                loaded_files.extend(loaded)
            elif lower.endswith((".xls", ".xlsx")):
                df_xls = load_single_excel_bytes(file_bytes, file_name)
                if not df_xls.empty:
                    frames.append(df_xls)
                    loaded_files.append(file_name)
        except Exception:
            continue

    if frames:
        df = pd.concat(frames, ignore_index=True)
        df["반납일"] = pd.to_datetime(df["반납일"], errors="coerce")
        return df, sorted(set(loaded_files))
    return pd.DataFrame(columns=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]), loaded_files


def combined_data(uploaded_files) -> tuple[pd.DataFrame, list[str]]:
    default_df, default_loaded = load_default_data()
    uploaded_df, uploaded_loaded = load_uploaded_files(uploaded_files)
    frames = [default_df]
    if not uploaded_df.empty:
        frames.append(uploaded_df)
    all_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not all_df.empty:
        all_df["반납일"] = pd.to_datetime(all_df["반납일"], errors="coerce")
        all_df = all_df.dropna(subset=["반납일"]).drop_duplicates(
            subset=["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]
        ).reset_index(drop=True)
    return all_df, sorted(set(default_loaded + uploaded_loaded))


def filter_df(df: pd.DataFrame, search_mode: str, start_date, end_date, single_date, manual_text: str, vendor_keyword: str, product_keyword: str):
    filtered = df.copy()
    if filtered.empty:
        return filtered, None

    filtered["반납일"] = pd.to_datetime(filtered["반납일"], errors="coerce")
    filtered = filtered.dropna(subset=["반납일"])

    if search_mode == "기간 검색" and start_date and end_date:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        filtered = filtered[(filtered["반납일"] >= start_ts) & (filtered["반납일"] <= end_ts)]
    elif search_mode == "하루 검색" and single_date:
        one = pd.Timestamp(single_date)
        filtered = filtered[filtered["반납일"] == one]
    elif search_mode == "수기 입력(~)":
        text = manual_text.strip()
        if text:
            match = re.match(r"\s*(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})\s*$", text)
            if not match:
                return filtered.iloc[0:0], "수기 입력은 2026-03-01~2026-03-20 형식으로 입력해주세요."
            start_ts = pd.Timestamp(match.group(1))
            end_ts = pd.Timestamp(match.group(2))
            filtered = filtered[(filtered["반납일"] >= start_ts) & (filtered["반납일"] <= end_ts)]

    vendor_keyword = normalize_text(vendor_keyword)
    product_keyword = normalize_text(product_keyword)

    if vendor_keyword:
        vendor_series = filtered["업체명"].astype(str).map(normalize_text)
        filtered = filtered[vendor_series.str.contains(re.escape(vendor_keyword), na=False, regex=True)]

    if product_keyword:
        product_series = filtered["엑셀 기입 내용"].astype(str).map(normalize_text)
        filtered = filtered[product_series.str.contains(re.escape(product_keyword), na=False, regex=True)]

    filtered = filtered.sort_values(by=["반납일", "업체명", "원본파일"], ascending=[False, True, True]).reset_index(drop=True)
    return filtered, None


if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = 0
if "search_triggered" not in st.session_state:
    st.session_state.search_triggered = False

st.title("미샵 샘플 반품 관리 프로그램")
left_top, right_top = st.columns([1, 4])
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
    help="기본 포함 파일 외에 새 반납 리스트를 계속 추가 업로드할 수 있습니다.",
)

all_df, loaded_files = combined_data(uploaded_files)

today = date.today()
default_start = today.replace(day=1)
default_end = today

st.markdown('<div class="search-card">', unsafe_allow_html=True)
st.caption("반납일 검색 방식")
search_mode = st.radio("반납일 검색 방식", ["기간 검색", "하루 검색", "수기 입력(~)", "전체"], index=0, horizontal=True, label_visibility="collapsed")

col1, col2, col3, col4 = st.columns([1.8, 1.2, 1.5, 0.7])
with col1:
    if search_mode == "기간 검색":
        date_range = st.date_input("반납일 범위", value=(default_start, default_end), format="YYYY-MM-DD")
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
        elif isinstance(date_range, list) and len(date_range) == 2:
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
        manual_text = st.text_input("반납일 범위", value=f"{default_start.isoformat()}~{default_end.isoformat()}", placeholder="2026-03-01~2026-03-20")
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
        st.info("등록된 샘플 반납 리스트에서 일치하는 내역이 없습니다.")
    else:
        display_df = filtered_df.copy()
        display_df["반납일"] = display_df["반납일"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.caption(f"검색 결과 {len(display_df):,}건")
else:
    st.caption("검색 조건을 입력한 뒤 검색 버튼을 눌러주세요.")

with st.expander("프로그램 안내"):
    st.write("- 처음 제공된 파일 전체를 기본 데이터로 함께 읽습니다.")
    st.write("- 업체명만 입력해도 검색되고, 상품명/내용만 입력해도 검색됩니다.")
    st.write("- 기간 검색은 현재 월 1일 ~ 오늘을 기본값으로 사용합니다.")
    st.write(f"- 현재 기본 등록 파일 수: {len(loaded_files)}개")
    st.write(f"- 현재 기본 등록 건수: {len(all_df):,}건")

st.markdown("<div style='margin-top:24px; color:#666; font-size:12px;'>copyright made by MISHARP COMPANY. MIYAWA. 2026</div>", unsafe_allow_html=True)
