import hashlib
import io
import os
import re
import sqlite3
import zipfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="미샵 샘플 반품 관리 프로그램", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 4.2rem; padding-bottom: 2rem;}
    .search-card {
        border: 1px solid #e9e9e9;
        border-radius: 16px;
        padding: 18px 18px 10px 18px;
        background: #ffffff;
        margin-bottom: 18px;
    }
    .small-note {color:#666; font-size:12px;}
    </style>
    """,
    unsafe_allow_html=True,
)

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"                         # 최초 기본 엑셀 파일
UPLOAD_DIR = APP_DIR / "persistent_uploads"         # 업로드 원본 보관
DB_PATH = APP_DIR / "sample_returns.db"              # 누적 DB
UPLOAD_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

COLUMNS = ["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]


def normalize_text(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    return re.sub(r"\s+", " ", text)


def compact_text(value) -> str:
    """검색 안정화를 위해 공백을 제거한 비교용 텍스트."""
    return re.sub(r"\s+", "", normalize_text(value)).lower()


def safe_filename(name: str) -> str:
    base = os.path.basename(name).replace("\\", "_").replace("/", "_")
    base = re.sub(r"[^0-9A-Za-z가-힣._()\- ]+", "_", base).strip()
    return base or "uploaded_file"


def file_digest(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def extract_date(raw: pd.DataFrame, source_name: str):
    # 1) 엑셀 상단 제목에서 날짜 추출
    title_candidates = []
    if raw.shape[0] > 0:
        for r in range(min(raw.shape[0], 3)):
            for c in range(min(raw.shape[1], 6)):
                title_candidates.append(normalize_text(raw.iat[r, c]))
    title = " ".join(title_candidates)
    patterns = [
        r"(20\d{2})[-./년\s]+(\d{1,2})[-./월\s]+(\d{1,2})",
        r"(20\d{2})(\d{2})(\d{2})",
    ]
    for pat in patterns:
        m = re.search(pat, title)
        if m:
            y, mth, d = map(int, m.groups())
            try:
                return pd.Timestamp(y, mth, d)
            except Exception:
                pass

    # 2) 파일명에서 날짜 추출
    base = os.path.basename(source_name)
    for pat in [r"(20\d{2})[-._ ]?(\d{1,2})[-._ ]?(\d{1,2})", r"(20\d{2})(\d{2})(\d{2})"]:
        m = re.search(pat, base)
        if m:
            y, mth, d = map(int, m.groups())
            try:
                return pd.Timestamp(y, mth, d)
            except Exception:
                pass

    # 3) 03-05.xls 형태는 현재 연도 기준으로 추정
    m2 = re.search(r"(?<!\d)(\d{1,2})[-./](\d{1,2})(?!\d)", base)
    if m2:
        mth, d = map(int, m2.groups())
        today = date.today()
        for year in [today.year, today.year - 1, today.year + 1]:
            try:
                return pd.Timestamp(year, mth, d)
            except Exception:
                continue
    return pd.NaT


def parse_excel_bytes(file_bytes: bytes, source_name: str) -> pd.DataFrame:
    suffix = os.path.splitext(source_name)[1].lower()
    engine = "xlrd" if suffix == ".xls" else "openpyxl"
    try:
        raw = pd.read_excel(io.BytesIO(file_bytes), header=None, engine=engine)
    except Exception:
        return pd.DataFrame(columns=COLUMNS)

    if raw.empty or raw.shape[0] < 2:
        return pd.DataFrame(columns=COLUMNS)

    return_date = extract_date(raw, source_name)

    # 헤더 행 자동 탐색
    header_row = 1
    for idx in range(min(len(raw), 8)):
        row_text = " ".join(normalize_text(v) for v in raw.iloc[idx].tolist())
        if any(k in row_text for k in ["거래처", "업체", "주소", "내용", "상품", "품목"]):
            header_row = idx
            break

    headers = [normalize_text(v) for v in raw.iloc[header_row].tolist()]
    headers = [h if h else f"col_{i}" for i, h in enumerate(headers)]
    body = raw.iloc[header_row + 1 :].copy()
    body.columns = headers
    body = body.dropna(how="all")
    if body.empty:
        return pd.DataFrame(columns=COLUMNS)

    col_vendor = None
    col_addr = None
    col_content = None
    for col in body.columns:
        key = normalize_text(col)
        if key in {"거래처명", "업체명", "거래처", "업체", "상호", "도매처", "도매처명"} and col_vendor is None:
            col_vendor = col
        elif key in {"주소", "위치"} and col_addr is None:
            col_addr = col
        elif key in {"내용", "엑셀 기입 내용", "상품명", "상품", "내역", "품목", "반납내용"} and col_content is None:
            col_content = col

    # 미샵 기존 양식 fallback: 보통 2번째 열 거래처, 3번째 주소, 4번째 내용
    cols = list(body.columns)
    if col_vendor is None and len(cols) >= 2:
        col_vendor = cols[1]
    if col_addr is None and len(cols) >= 3:
        col_addr = cols[2]
    if col_content is None and len(cols) >= 4:
        col_content = cols[3]

    result = pd.DataFrame(
        {
            "반납일": [return_date] * len(body),
            "업체명": body[col_vendor].map(normalize_text) if col_vendor is not None else "",
            "주소": body[col_addr].map(normalize_text) if col_addr is not None else "",
            "엑셀 기입 내용": body[col_content].map(normalize_text) if col_content is not None else "",
            "원본파일": os.path.basename(source_name),
        }
    )
    result = result[(result["업체명"] != "") | (result["엑셀 기입 내용"] != "")]
    result["반납일"] = pd.to_datetime(result["반납일"], errors="coerce")
    result = result.dropna(subset=["반납일"])
    return result.reset_index(drop=True)


def parse_zip_bytes(file_bytes: bytes, source_name: str = "uploaded.zip") -> tuple[pd.DataFrame, list[str]]:
    frames = []
    names = []
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            for name in zf.namelist():
                base = os.path.basename(name)
                if not base or base.startswith("~$"):
                    continue
                if name.lower().endswith((".xls", ".xlsx")):
                    data = zf.read(name)
                    df = parse_excel_bytes(data, name)
                    if not df.empty:
                        frames.append(df)
                    names.append(base)
    except Exception:
        return pd.DataFrame(columns=COLUMNS), names
    if frames:
        return pd.concat(frames, ignore_index=True), names
    return pd.DataFrame(columns=COLUMNS), names


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sample_returns (
                row_hash TEXT PRIMARY KEY,
                return_date TEXT,
                vendor TEXT,
                address TEXT,
                content TEXT,
                source_file TEXT,
                imported_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS imported_files (
                file_hash TEXT PRIMARY KEY,
                filename TEXT,
                saved_name TEXT,
                imported_at TEXT,
                row_count INTEGER
            )
            """
        )
        conn.commit()


def make_row_hash(row) -> str:
    key = "|".join(
        [
            str(row.get("반납일", ""))[:10],
            normalize_text(row.get("업체명", "")),
            normalize_text(row.get("주소", "")),
            normalize_text(row.get("엑셀 기입 내용", "")),
            normalize_text(row.get("원본파일", "")),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def insert_dataframe_to_db(df: pd.DataFrame, source_file_hash: str = "") -> int:
    if df.empty:
        return 0
    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    clean = df.copy()
    clean["반납일"] = pd.to_datetime(clean["반납일"], errors="coerce")
    clean = clean.dropna(subset=["반납일"])
    for _, row in clean.iterrows():
        rows.append(
            (
                make_row_hash(row),
                row["반납일"].strftime("%Y-%m-%d"),
                normalize_text(row.get("업체명", "")),
                normalize_text(row.get("주소", "")),
                normalize_text(row.get("엑셀 기입 내용", "")),
                normalize_text(row.get("원본파일", "")),
                now,
            )
        )
    with sqlite3.connect(DB_PATH) as conn:
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR IGNORE INTO sample_returns
            (row_hash, return_date, vendor, address, content, source_file, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return conn.total_changes - before


def record_imported_file(file_hash: str, filename: str, saved_name: str, row_count: int):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO imported_files
            (file_hash, filename, saved_name, imported_at, row_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (file_hash, filename, saved_name, now, row_count),
        )
        conn.commit()


def get_file_already_imported(file_hash: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT 1 FROM imported_files WHERE file_hash = ?", (file_hash,))
        return cur.fetchone() is not None


def seed_default_data_if_needed():
    """data 폴더의 기본 엑셀을 DB에 누적 등록. 중복은 row_hash로 자동 제외."""
    init_db()
    for path in sorted(DATA_DIR.iterdir()):
        if path.name.startswith("~$") or path.suffix.lower() not in [".xls", ".xlsx", ".zip"]:
            continue
        data = path.read_bytes()
        digest = file_digest(data)
        if get_file_already_imported(digest):
            continue
        if path.suffix.lower() == ".zip":
            df, _ = parse_zip_bytes(data, path.name)
        else:
            df = parse_excel_bytes(data, path.name)
        inserted = insert_dataframe_to_db(df, digest)
        record_imported_file(digest, path.name, f"data/{path.name}", len(df))


def save_and_import_upload(uploaded_files) -> tuple[int, int, int]:
    """업로드 파일 저장 + DB 누적. 반환: 저장파일 수, 전체 파싱행, 신규등록행."""
    saved_count = 0
    parsed_rows = 0
    inserted_rows = 0
    for up in uploaded_files or []:
        if not up.name.lower().endswith((".zip", ".xls", ".xlsx")):
            continue
        data = up.getvalue()
        digest = file_digest(data)
        name = safe_filename(up.name)
        stem, ext = os.path.splitext(name)
        target = UPLOAD_DIR / f"{stem}_{digest[:10]}{ext}"
        if not target.exists():
            target.write_bytes(data)
        saved_count += 1

        # 이미 같은 파일을 가져온 적이 있더라도, 혹시 이전 import 실패가 있었을 수 있으므로 파싱은 수행하고 row_hash로 중복 제거
        if ext.lower() == ".zip":
            df, _ = parse_zip_bytes(data, up.name)
        else:
            df = parse_excel_bytes(data, up.name)
        parsed_rows += len(df)
        inserted = insert_dataframe_to_db(df, digest)
        inserted_rows += inserted
        record_imported_file(digest, up.name, str(target.relative_to(APP_DIR)), len(df))
    return saved_count, parsed_rows, inserted_rows


@st.cache_data(show_spinner=False)
def load_db_dataframe() -> pd.DataFrame:
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            """
            SELECT
                return_date AS 반납일,
                vendor AS 업체명,
                address AS 주소,
                content AS '엑셀 기입 내용',
                source_file AS 원본파일,
                imported_at AS 등록일시
            FROM sample_returns
            ORDER BY return_date DESC, vendor ASC
            """,
            conn,
        )
    if not df.empty:
        df["반납일"] = pd.to_datetime(df["반납일"], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_imported_files_info() -> pd.DataFrame:
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(
            "SELECT filename, saved_name, imported_at, row_count FROM imported_files ORDER BY imported_at DESC",
            conn,
        )


def get_default_range(df: pd.DataFrame):
    today = pd.Timestamp(date.today())
    if df.empty:
        return today.replace(day=1).date(), today.date(), "현재 월 기준"
    tmp = df.copy()
    tmp["반납일"] = pd.to_datetime(tmp["반납일"], errors="coerce")
    tmp = tmp.dropna(subset=["반납일"])
    if tmp.empty:
        return today.replace(day=1).date(), today.date(), "현재 월 기준"
    current_month = tmp[(tmp["반납일"].dt.year == today.year) & (tmp["반납일"].dt.month == today.month)]
    if not current_month.empty:
        return today.replace(day=1).date(), today.date(), "현재 월 기준"
    latest = tmp["반납일"].max()
    return latest.replace(day=1).date(), latest.date(), f"데이터 최신 월 기준 ({latest.strftime('%Y-%m')})"


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
                return filtered.iloc[0:0], "수기 입력은 2026-03-01~2026-03-20 형식으로 입력해주세요."
            s = pd.Timestamp(m.group(1))
            e = pd.Timestamp(m.group(2))
            filtered = filtered[(filtered["반납일"] >= s) & (filtered["반납일"] <= e)]

    vendor_keyword = compact_text(vendor_keyword)
    product_keyword = compact_text(product_keyword)

    if vendor_keyword:
        vendor_series = filtered["업체명"].astype(str).map(compact_text)
        filtered = filtered[vendor_series.str.contains(vendor_keyword, na=False, regex=False)]

    if product_keyword:
        content_series = filtered["엑셀 기입 내용"].astype(str).map(compact_text)
        filtered = filtered[content_series.str.contains(product_keyword, na=False, regex=False)]

    filtered = filtered.sort_values(["반납일", "업체명", "원본파일"], ascending=[False, True, True]).reset_index(drop=True)
    return filtered, None


# 앱 시작 시 기본 데이터 DB 등록
seed_default_data_if_needed()

if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = 0
if "search_triggered" not in st.session_state:
    st.session_state.search_triggered = False
if "upload_msg" not in st.session_state:
    st.session_state.upload_msg = ""

st.title("미샵 샘플 반품 관리 프로그램")
st.caption("엑셀/ZIP 파일을 업로드하면 DB에 누적 저장되고, 새로고침 후에도 검색됩니다.")

uploaded_files = st.file_uploader(
    "샘플반납 엑셀 파일 업로드 (ZIP / XLS / XLSX)",
    type=["zip", "xls", "xlsx"],
    accept_multiple_files=True,
    key=f"uploader_{st.session_state.uploader_nonce}",
    help="업로드한 파일은 persistent_uploads 폴더에 보관되고, 내용은 sample_returns.db에 누적 저장됩니다.",
)

if uploaded_files:
    saved_count, parsed_rows, inserted_rows = save_and_import_upload(uploaded_files)
    st.session_state.uploader_nonce += 1
    st.session_state.search_triggered = False
    st.session_state.upload_msg = f"업로드 {saved_count}개 완료 / 파싱 {parsed_rows:,}건 / 신규 DB 등록 {inserted_rows:,}건"
    st.cache_data.clear()
    st.rerun()

if st.session_state.upload_msg:
    st.success(st.session_state.upload_msg)
    st.session_state.upload_msg = ""

all_df = load_db_dataframe()
files_info = load_imported_files_info()
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
        if not all_df.empty:
            st.caption(f"등록 데이터 기간: {all_df['반납일'].min().strftime('%Y-%m-%d')} ~ {all_df['반납일'].max().strftime('%Y-%m-%d')}")
    else:
        display_df = filtered_df[["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]].copy()
        display_df["반납일"] = display_df["반납일"].dt.strftime("%Y-%m-%d")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.caption(f"검색 결과 {len(display_df):,}건")
else:
    st.caption("검색 조건을 입력한 뒤 검색 버튼을 눌러주세요.")

with st.expander("프로그램 안내 / 등록 현황"):
    st.write("- 업로드한 파일은 `persistent_uploads` 폴더에 원본 저장됩니다.")
    st.write("- 검색 데이터는 `sample_returns.db` SQLite DB에 누적 저장됩니다.")
    st.write("- 새로고침하거나 프로그램을 다시 실행해도 DB 파일이 남아 있으면 검색 데이터가 유지됩니다.")
    st.write("- 업체명만 입력해도 검색되고, 상품명/내용만 입력해도 검색됩니다.")
    st.write(f"- 기간 검색 기본값은 {default_label}으로 자동 설정됩니다.")
    if not all_df.empty:
        st.write(f"- 등록 데이터 기간: {all_df['반납일'].min().strftime('%Y-%m-%d')} ~ {all_df['반납일'].max().strftime('%Y-%m-%d')}")
    st.write(f"- 현재 DB 등록 건수: {len(all_df):,}건")
    st.write(f"- 현재 등록 파일 수: {len(files_info):,}개")
    st.write(f"- DB 파일 위치: `{DB_PATH.name}`")
    st.warning("Streamlit Cloud에서 앱을 재배포하거나 서버 저장공간이 초기화되면 로컬 DB 파일이 사라질 수 있습니다. 완전 운영형은 Supabase/Google Sheets 같은 외부 DB 연결이 필요합니다.")
    if not files_info.empty:
        st.dataframe(files_info.head(30), use_container_width=True, hide_index=True)

st.markdown("<div style='margin-top:24px; color:#666; font-size:12px;'>copyright made by MISHARP COMPANY. MIYAWA. 2026</div>", unsafe_allow_html=True)
