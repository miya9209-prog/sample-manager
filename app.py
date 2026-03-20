from __future__ import annotations

import hashlib
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
LOG_DIR = BASE_DIR / "logs"
STATUS_CSV = LOG_DIR / "return_status_log.csv"
DEFAULT_FILES = [
    DATA_DIR / "샘플 반납 리스트.zip",
    DATA_DIR / "01-09.xls",
]
LOG_COLUMNS = [
    "row_key",
    "상태",
    "처리일시",
    "처리자",
    "메모",
    "반납일",
    "업체명",
    "주소",
    "상품내용",
    "원본파일",
]

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.6rem; padding-bottom: 3.2rem; max-width: 1450px;}
    .main-title {font-size: 2rem; font-weight: 800; margin-bottom: 0.25rem;}
    .sub-title {color: #555; font-size: 1rem; margin-bottom: 1rem;}
    .info-card {
        border: 1px solid #e7e7e7; border-radius: 18px; padding: 18px 20px;
        background: #fafafa; margin-bottom: 12px;
    }
    .footer-copy {
        margin-top: 28px; padding-top: 16px; border-top: 1px solid #e7e7e7;
        text-align: center; color: #666; font-size: 0.92rem;
    }
    .status-done {
        display:inline-block; padding: 4px 10px; border-radius: 999px;
        background:#ecfdf3; color:#067647; font-weight:700; font-size:0.9rem;
    }
    .status-pending {
        display:inline-block; padding: 4px 10px; border-radius: 999px;
        background:#fff4ed; color:#b42318; font-weight:700; font-size:0.9rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    text = re.sub(r"\s+", " ", text)
    return text



def safe_date_string(value: object) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)



def parse_date_from_text(text: str) -> Optional[date]:
    if not text:
        return None
    text = str(text)
    patterns = [
        r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})",
        r"(20\d{2})(\d{2})(\d{2})",
        r"(\d{1,2})[-./](\d{1,2})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if not m:
            continue
        groups = m.groups()
        try:
            if len(groups) == 3 and len(groups[0]) == 4:
                y, mth, d = map(int, groups)
                return date(y, mth, d)
            if len(groups) == 2:
                current_year = datetime.now().year
                mth, d = map(int, groups)
                return date(current_year, mth, d)
        except Exception:
            continue
    return None



def extract_return_date(df_raw: pd.DataFrame, source_name: str) -> Optional[date]:
    candidates = []
    if not df_raw.empty:
        candidates.append(normalize_text(df_raw.iat[0, 0]))
        if df_raw.shape[1] > 1:
            candidates.append(normalize_text(df_raw.iat[0, 1]))
    candidates.append(source_name)
    for c in candidates:
        found = parse_date_from_text(c)
        if found:
            return found
    return None



def make_row_key(return_date: object, vendor: object, address: object, content: object, source_name: object) -> str:
    raw = "||".join(
        [
            safe_date_string(return_date),
            normalize_text(vendor),
            normalize_text(address),
            normalize_text(content),
            normalize_text(source_name),
        ]
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()



def read_excel_bytes(file_bytes: bytes, source_name: str) -> pd.DataFrame:
    raw = pd.read_excel(io.BytesIO(file_bytes), header=None)
    if raw.empty:
        return pd.DataFrame()

    return_date = extract_return_date(raw, source_name)

    rows = []
    for idx in range(2, len(raw)):
        serial = normalize_text(raw.iat[idx, 0]) if raw.shape[1] > 0 else ""
        vendor = normalize_text(raw.iat[idx, 1]) if raw.shape[1] > 1 else ""
        address = normalize_text(raw.iat[idx, 2]) if raw.shape[1] > 2 else ""
        content = normalize_text(raw.iat[idx, 3]) if raw.shape[1] > 3 else ""

        if not any([serial, vendor, address, content]):
            continue
        if vendor == "거래처명" and content == "내용":
            continue

        rows.append(
            {
                "row_key": make_row_key(return_date, vendor, address, content, source_name),
                "반납일": return_date,
                "반납일표기": safe_date_string(return_date),
                "순번": serial,
                "업체명": vendor,
                "주소": address,
                "상품내용": content,
                "원본파일": source_name,
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df["검색용텍스트"] = (
            df[["업체명", "주소", "상품내용", "원본파일"]]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.lower()
        )
    return df



def load_from_zip_bytes(file_bytes: bytes) -> list[pd.DataFrame]:
    results: list[pd.DataFrame] = []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            lower = member.filename.lower()
            if not (lower.endswith(".xls") or lower.endswith(".xlsx")):
                continue
            member_bytes = zf.read(member)
            member_name = Path(member.filename).name
            try:
                df = read_excel_bytes(member_bytes, member_name)
                if not df.empty:
                    results.append(df)
            except Exception:
                continue
    return results



def load_local_defaults() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in DEFAULT_FILES:
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".zip":
                frames.extend(load_from_zip_bytes(path.read_bytes()))
            else:
                df = read_excel_bytes(path.read_bytes(), path.name)
                if not df.empty:
                    frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["row_key", "반납일", "반납일표기", "순번", "업체명", "주소", "상품내용", "원본파일", "검색용텍스트"])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["row_key"])
    merged = merged.sort_values(by=["반납일표기", "업체명", "순번"], ascending=[False, True, True], kind="stable")
    return merged.reset_index(drop=True)



def load_uploaded_files(uploaded_files) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for uploaded in uploaded_files or []:
        try:
            file_bytes = uploaded.read()
            lower = uploaded.name.lower()
            if lower.endswith(".zip"):
                frames.extend(load_from_zip_bytes(file_bytes))
            elif lower.endswith((".xls", ".xlsx")):
                df = read_excel_bytes(file_bytes, uploaded.name)
                if not df.empty:
                    frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=["row_key", "반납일", "반납일표기", "순번", "업체명", "주소", "상품내용", "원본파일", "검색용텍스트"])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["row_key"])
    merged = merged.sort_values(by=["반납일표기", "업체명", "순번"], ascending=[False, True, True], kind="stable")
    return merged.reset_index(drop=True)



def combine_datasets(base_df: pd.DataFrame, uploaded_df: pd.DataFrame) -> pd.DataFrame:
    if base_df.empty and uploaded_df.empty:
        return base_df
    if uploaded_df.empty:
        return base_df.copy()
    merged = pd.concat([base_df, uploaded_df], ignore_index=True)
    merged = merged.drop_duplicates(subset=["row_key"])
    merged = merged.sort_values(by=["반납일표기", "업체명", "순번"], ascending=[False, True, True], kind="stable")
    return merged.reset_index(drop=True)



def parse_manual_date_range(text: str) -> tuple[Optional[date], Optional[date], Optional[str]]:
    text = (text or "").strip()
    if not text:
        return None, None, None

    parts = re.split(r"\s*~\s*", text)
    if len(parts) == 1:
        d = parse_date_from_text(parts[0])
        if d is None:
            return None, None, "날짜 형식을 확인해주세요. 예: 2026-03-01~2026-03-20"
        return d, d, None
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

    if not filtered.empty:
        if mode == "하루 검색" and single_date:
            filtered = filtered[filtered["반납일"] == single_date]
        elif mode == "기간 검색":
            start, end = range_dates
            if start and end:
                filtered = filtered[(filtered["반납일"] >= start) & (filtered["반납일"] <= end)]
        elif mode == "수기 입력(~)":
            start, end, error_msg = parse_manual_date_range(manual_range_text)
            if not error_msg and start and end:
                filtered = filtered[(filtered["반납일"] >= start) & (filtered["반납일"] <= end)]

        vendor_keyword = vendor_keyword.strip().lower()
        product_keyword = product_keyword.strip().lower()

        if vendor_keyword:
            filtered = filtered[filtered["업체명"].fillna("").str.lower().str.contains(vendor_keyword, na=False)]
        if product_keyword:
            filtered = filtered[filtered["상품내용"].fillna("").str.lower().str.contains(product_keyword, na=False)]

    return filtered.reset_index(drop=True), error_msg



def ensure_status_store() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if not STATUS_CSV.exists():
        pd.DataFrame(columns=LOG_COLUMNS).to_csv(STATUS_CSV, index=False, encoding="utf-8-sig")



def load_status_log() -> pd.DataFrame:
    ensure_status_store()
    try:
        log_df = pd.read_csv(STATUS_CSV, dtype=str).fillna("")
    except Exception:
        log_df = pd.DataFrame(columns=LOG_COLUMNS)
    for col in LOG_COLUMNS:
        if col not in log_df.columns:
            log_df[col] = ""
    return log_df[LOG_COLUMNS].copy()



def save_status_log(log_df: pd.DataFrame) -> None:
    ensure_status_store()
    log_df[LOG_COLUMNS].to_csv(STATUS_CSV, index=False, encoding="utf-8-sig")



def upsert_return_status(selected_rows: pd.DataFrame, processor: str, memo: str) -> int:
    log_df = load_status_log()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    processor = processor.strip() or "담당자 미입력"
    memo = memo.strip()

    existing_keys = set(log_df["row_key"].astype(str).tolist())
    new_rows = []
    for _, row in selected_rows.iterrows():
        record = {
            "row_key": row["row_key"],
            "상태": "반품완료",
            "처리일시": now_str,
            "처리자": processor,
            "메모": memo,
            "반납일": row["반납일표기"],
            "업체명": row["업체명"],
            "주소": row["주소"],
            "상품내용": row["상품내용"],
            "원본파일": row["원본파일"],
        }
        if record["row_key"] in existing_keys:
            mask = log_df["row_key"].astype(str) == record["row_key"]
            for key, value in record.items():
                log_df.loc[mask, key] = value
        else:
            new_rows.append(record)

    if new_rows:
        log_df = pd.concat([log_df, pd.DataFrame(new_rows)], ignore_index=True)

    log_df = log_df.drop_duplicates(subset=["row_key"], keep="last")
    save_status_log(log_df)
    return len(selected_rows)



def merge_status(df: pd.DataFrame, status_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    merged = df.merge(
        status_df[["row_key", "상태", "처리일시", "처리자", "메모"]],
        on="row_key",
        how="left",
    )
    merged["상태"] = merged["상태"].fillna("미반품")
    merged["처리일시"] = merged["처리일시"].fillna("")
    merged["처리자"] = merged["처리자"].fillna("")
    merged["메모"] = merged["메모"].fillna("")
    merged["상태표시"] = merged["상태"].map(lambda x: "✅ 반품완료" if x == "반품완료" else "❌ 미반품")
    return merged


@st.cache_data(show_spinner=False)
def get_default_dataset() -> pd.DataFrame:
    return load_local_defaults()


if "uploader_nonce" not in st.session_state:
    st.session_state.uploader_nonce = 0

if st.button("새 작업 / 업로드 초기화"):
    st.session_state.uploader_nonce += 1
    st.rerun()

status_log = load_status_log()
default_df = get_default_dataset()

st.markdown('<div class="main-title">미샵 샘플 반품 관리 프로그램</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-title">업체명, 상품명, 반납일 기준으로 샘플 반납 내역을 조회하고 CSV 로그로 반품 처리 기록까지 남길 수 있습니다.</div>',
    unsafe_allow_html=True,
)

with st.expander("프로그램 안내", expanded=True):
    st.markdown(
        """
        - 처음 제공해주신 ZIP/XLS 파일을 기본 데이터로 등록해두었습니다.
        - 이후 새 XLS/XLSX/ZIP 파일을 업로드하면 기존 데이터와 함께 바로 검색할 수 있습니다.
        - 검색 결과에서 필요한 행을 선택한 뒤 **반품 완료 처리**를 누르면 CSV 로그에 기록됩니다.
        - 이 기록은 프로그램을 다시 열어도 유지되어, 간단한 분쟁 방지용 증빙으로 활용할 수 있습니다.
        """
    )

uploaded_files = st.file_uploader(
    "반납 리스트 업로드 (XLS, XLSX, ZIP / 여러 개 가능)",
    type=["xls", "xlsx", "zip"],
    accept_multiple_files=True,
    key=f"upload_{st.session_state.uploader_nonce}",
    help="새 파일을 추가 업로드하면 기본 데이터와 합쳐서 검색합니다.",
)

uploaded_df = load_uploaded_files(uploaded_files)
all_df = combine_datasets(default_df, uploaded_df)
all_df = merge_status(all_df, status_log)

left, mid, right, four = st.columns([1, 1, 1, 1])
with left:
    st.markdown(f'<div class="info-card"><b>기본 등록 건수</b><br>{len(default_df):,}건</div>', unsafe_allow_html=True)
with mid:
    st.markdown(f'<div class="info-card"><b>추가 업로드 건수</b><br>{len(uploaded_df):,}건</div>', unsafe_allow_html=True)
with right:
    completed_count = int((all_df["상태"] == "반품완료").sum()) if not all_df.empty else 0
    st.markdown(f'<div class="info-card"><b>반품 완료 기록</b><br>{completed_count:,}건</div>', unsafe_allow_html=True)
with four:
    st.markdown(f'<div class="info-card"><b>현재 검색 가능 총건수</b><br>{len(all_df):,}건</div>', unsafe_allow_html=True)

st.markdown("### 검색창")
search_mode = st.radio(
    "반납일 검색 방식",
    ["전체", "하루 검색", "기간 검색", "수기 입력(~)"],
    horizontal=True,
)

c1, c2, c3 = st.columns([1.1, 1, 1.3])
min_loaded_date = all_df["반납일"].dropna().min() if not all_df.empty and all_df["반납일"].notna().any() else date.today()
max_loaded_date = all_df["반납일"].dropna().max() if not all_df.empty and all_df["반납일"].notna().any() else date.today()

with c1:
    single_date = st.date_input("반납일", value=max_loaded_date, format="YYYY-MM-DD") if search_mode == "하루 검색" else None
    range_dates = (None, None)
    manual_range_text = ""
    if search_mode == "기간 검색":
        range_dates = st.date_input(
            "반납일 범위",
            value=(min_loaded_date, max_loaded_date),
            format="YYYY-MM-DD",
        )
    elif search_mode == "수기 입력(~)":
        manual_range_text = st.text_input(
            "반납일 수기 입력",
            placeholder="예: 2026-03-01~2026-03-20",
        )
with c2:
    vendor_keyword = st.text_input("업체명", placeholder="예: 까르르")
with c3:
    product_keyword = st.text_input("상품명 / 내용", placeholder="예: 맨투맨, 코트, 슬랙스")

filtered_df, filter_error = filter_df(
    all_df,
    search_mode,
    single_date,
    range_dates if isinstance(range_dates, tuple) else tuple(range_dates),
    manual_range_text,
    vendor_keyword,
    product_keyword,
)

st.markdown("### 검색 결과")
search_used = any([search_mode != "전체", bool(str(vendor_keyword).strip()), bool(str(product_keyword).strip())])

if filter_error:
    st.error(filter_error)
elif search_used and filtered_df.empty:
    st.warning("검색 조건에 해당하는 반납 내역이 없습니다. 현재 반납리스트에서 확인되지 않습니다.")
elif filtered_df.empty:
    st.info("표시할 데이터가 없습니다. 업로드 파일을 확인해주세요.")
else:
    st.success(f"검색 결과 {len(filtered_df):,}건이 확인되었습니다.")

    display_df = filtered_df[
        ["row_key", "상태표시", "반납일표기", "업체명", "주소", "상품내용", "원본파일", "처리일시", "처리자", "메모"]
    ].rename(
        columns={
            "상태표시": "상태",
            "반납일표기": "반납일",
            "상품내용": "액셀 기입 내용",
        }
    )

    edited = st.data_editor(
        display_df,
        use_container_width=True,
        hide_index=True,
        disabled=["상태", "반납일", "업체명", "주소", "액셀 기입 내용", "원본파일", "처리일시", "처리자", "메모", "row_key"],
        column_config={
            "row_key": None,
            "선택": st.column_config.CheckboxColumn("선택", help="반품 완료 처리할 행을 선택하세요."),
            "상태": st.column_config.TextColumn("상태"),
            "반납일": st.column_config.TextColumn("반납일"),
            "업체명": st.column_config.TextColumn("업체명"),
            "주소": st.column_config.TextColumn("주소"),
            "액셀 기입 내용": st.column_config.TextColumn("액셀 기입 내용", width="large"),
            "원본파일": st.column_config.TextColumn("원본파일"),
            "처리일시": st.column_config.TextColumn("처리일시"),
            "처리자": st.column_config.TextColumn("처리자"),
            "메모": st.column_config.TextColumn("메모"),
        },
        num_rows="fixed",
    )

    if "선택" not in edited.columns:
        edited["선택"] = False

    action_left, action_mid = st.columns([1.2, 1.8])
    with action_left:
        processor = st.text_input("처리자", placeholder="예: 형준 / 민지")
    with action_mid:
        memo = st.text_input("메모", placeholder="예: 1차 반품 완료 / 박스 발송")

    selected_keys = edited.loc[edited["선택"] == True, "row_key"].astype(str).tolist()
    selected_rows = filtered_df[filtered_df["row_key"].astype(str).isin(selected_keys)].copy()

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("선택 행 반품 완료 처리", type="primary", use_container_width=True):
            if selected_rows.empty:
                st.warning("먼저 반품 완료 처리할 행을 선택해주세요.")
            else:
                saved_count = upsert_return_status(selected_rows, processor, memo)
                st.success(f"{saved_count}건이 반품 완료로 기록되었습니다.")
                st.rerun()
    with col_b:
        csv_bytes = display_df.drop(columns=["row_key"], errors="ignore").to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            "검색결과 CSV 다운로드",
            data=csv_bytes,
            file_name=f"sample_return_search_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

st.markdown("### 반품 완료 히스토리")
status_log = load_status_log()
if status_log.empty:
    st.info("아직 반품 완료로 기록된 내역이 없습니다.")
else:
    history_df = status_log.sort_values(by="처리일시", ascending=False).rename(
        columns={
            "상태": "상태",
            "반납일": "반납일",
            "상품내용": "액셀 기입 내용",
        }
    )[["상태", "처리일시", "처리자", "업체명", "반납일", "액셀 기입 내용", "메모", "원본파일"]]
    st.dataframe(history_df, use_container_width=True, hide_index=True)

    history_csv = history_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button(
        "반품 완료 히스토리 CSV 다운로드",
        data=history_csv,
        file_name=f"sample_return_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=False,
    )

with st.expander("현재 등록된 원본 파일 보기"):
    file_list = sorted(all_df["원본파일"].dropna().astype(str).unique().tolist()) if not all_df.empty else []
    if file_list:
        st.write(" / ".join(file_list))
    else:
        st.write("등록된 파일이 없습니다.")

st.markdown(
    '<div class="footer-copy">copyright made by MISHARP COMPANY. MIYAWA. 2026</div>',
    unsafe_allow_html=True,
)
