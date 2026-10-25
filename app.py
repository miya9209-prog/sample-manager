"""MISHARP | Google Drive-backed sample return search (Streamlit Cloud)."""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from sample_core import (
    connect, default_dates, get_stats, read_records, search_records,
    seed_repository, sync_drive,
)

st.set_page_config(page_title="미샵 샘플 반품 관리 프로그램", page_icon="📦", layout="wide")
st.markdown("""<style>
.block-container{padding-top:4rem;padding-bottom:2.4rem;max-width:1300px}
[data-testid="stMetric"]{padding:0.4rem 0}
</style>""", unsafe_allow_html=True)

APP = Path(__file__).resolve().parent
DB = APP / '.cache' / 'sample_search.sqlite3'


def secret(name, default=None):
    try:
        return st.secrets.get(name, default)
    except (FileNotFoundError, KeyError):
        return default


def drive_config():
    try:
        section = dict(secret('gdrive', {}) or {})
        folder = str(secret('GDRIVE_FOLDER_ID') or secret('DRIVE_FOLDER_ID') or section.get('folder_id') or '').strip()
        info = secret('gcp_service_account') or secret('gcp_service_account_json') or section.get('service_account_json')
        if info is None:
            return folder, None, None
        if isinstance(info, str):
            info = json.loads(info.strip())
        info = dict(info)
        if info.get('private_key'):
            # Work with both correctly escaped JSON and accidental double-escaping.
            info['private_key'] = info['private_key'].replace('\\n', '\n')
        if not info.get('client_email') or not info.get('private_key'):
            return folder, None, '서비스 계정의 client_email 또는 private_key가 없습니다.'
        return folder, info, None
    except json.JSONDecodeError:
        return '', None, 'gcp_service_account_json은 유효한 JSON 형식이어야 합니다.'
    except Exception as exc:
        return '', None, f'Secrets 읽기 오류: {type(exc).__name__}'


def drive_service(project: str, email: str, private_key: str, info_json: str):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(info_json), scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)


def safe_error(err) -> str:
    """Never show key or entire credentials in UI error messages."""
    msg = str(err)
    if 'HttpError 404' in msg or 'notFound' in msg:
        return '구글드라이브 폴더를 찾을 수 없습니다. 폴더 ID와 서비스 계정 공유 권한을 확인하세요.'
    if 'HttpError 403' in msg or 'insufficientPermissions' in msg:
        return '구글드라이브 접근 권한이 없습니다. 서비스 계정 이메일을 해당 폴더에 공유했는지 확인하세요.'
    if 'invalid_grant' in msg or 'Could not deserialize key' in msg:
        return '서비스 계정 인증키가 올바르지 않습니다. Streamlit Secrets의 JSON 값을 확인하세요.'
    return f'{type(err).__name__}: {msg[:180]}'


def search_database():
    db = connect(DB)
    # Repository's existing files are a recoverable baseline, not permanent uploads.
    seed = seed_repository(db, APP / 'data')
    return db, seed


conn, base_report = search_database()
folder_id, credential_info, configuration_error = drive_config()
can_connect = bool(folder_id and credential_info and not configuration_error)

if 'last_sync' not in st.session_state:
    st.session_state.last_sync = None
if 'sync_report' not in st.session_state:
    st.session_state.sync_report = None
if 'sync_error' not in st.session_state:
    st.session_state.sync_error = None
if 'searched' not in st.session_state:
    st.session_state.searched = False
if 'last_results' not in st.session_state:
    st.session_state.last_results = None
if 'search_error' not in st.session_state:
    st.session_state.search_error = None


def perform_sync():
    if not can_connect:
        st.session_state.sync_error = configuration_error or 'Streamlit Secrets에 Google Drive 폴더 ID와 서비스 계정 설정이 필요합니다.'
        return False
    try:
        info_json = json.dumps(credential_info, ensure_ascii=False)
        service = drive_service(credential_info.get('project_id',''), credential_info['client_email'],
                                credential_info['private_key'], info_json)
        # Show progress, especially on first cold boot when Drive files are re-imported.
        bar = st.progress(0.0, text='구글드라이브 파일 목록을 확인 중입니다...')
        def show_progress(n, total, filename):
            bar.progress(n/max(total, 1), text=f'구글드라이브 {n}/{total}개 확인: {filename[:60]}')
        report = sync_drive(conn, service, folder_id, progress=show_progress)
        bar.empty()
        st.session_state.last_sync = time.time()
        st.session_state.sync_report = report
        st.session_state.sync_error = None
        return True
    except Exception as exc:
        st.session_state.sync_error = safe_error(exc)
        return False


st.title('미샵 샘플 반품 관리 프로그램')
st.caption('반품 완료 엑셀 검색 · Google Drive 원본 영구 보관 · 새로고침 시에도 원본에서 자동 복원')

# Automatic sync ONCE per browser session, with explicit sync on every search.
if can_connect and st.session_state.last_sync is None:
    with st.spinner('구글드라이브에 등록된 반납파일을 처음 불러오는 중입니다...'):
        perform_sync()

b1, b2 = st.columns([1, 3.8])
with b1:
    if st.button('구글드라이브 새로 동기화', use_container_width=True, disabled=not can_connect):
        perform_sync()
with b2:
    st.caption('직원은 구글드라이브 **샘플관리파일** 폴더에 XLS / XLSX / ZIP을 추가하면 됩니다. 검색 버튼을 누를 때도 새 파일을 확인합니다.')

if not can_connect:
    st.error(configuration_error or '구글드라이브 연결 정보가 설정되지 않았습니다. Streamlit Cloud → App settings → Secrets를 확인하세요.')
elif_err = st.session_state.sync_error
if elif_err:
    st.error(f'구글드라이브 동기화 실패: {elif_err}  현재 검색은 기본 파일 또는 이전에 동기화된 내용만 포함할 수 있습니다.')

report = st.session_state.sync_report
if report and report.file_count == 0:
    st.warning('연결된 구글드라이브 폴더에서 Excel/ZIP/Google Sheets 파일을 한 개도 찾지 못했습니다. 업로드한 폴더가 정확한지 확인하세요.')
if report:
    status = '완료' if not report.failed else '일부 파일 확인 필요'
    st.caption(f'구글드라이브 동기화 {status} · 발견 {report.file_count:,}개 · 신규/변경 처리 {report.new_files:,}개 · 이전 등록 유지 {report.unchanged_files:,}개 · 처리된 내역 {report.row_count:,}건')
    if report.failed:
        st.warning(f'**읽기 실패한 파일 {len(report.failed)}개**가 있습니다. 아래 「등록 현황 / 문제 파일」에서 확인하세요. 실패 파일은 등록 완료로 표시하지 않습니다.')

if base_report.failed:
    st.warning(f'GitHub 기본 엑셀 중 {len(base_report.failed)}개를 읽지 못했습니다. 하단 등록 현황에서 확인하세요.')

stats = get_stats(conn)
metrics = st.columns(3)
metrics[0].metric('검색 가능한 내역', f'{stats["rows"]:,}건')
metrics[1].metric('Google Drive 등록 파일', f'{stats["drive_files"]:,}개')
metrics[2].metric('전체 등록 파일', f'{stats["files"]:,}개')

start_default, end_default, period_label = default_dates(conn)
if period_label != '이번 달':
    st.caption(f'기본 조회 기간: {period_label} ({start_default} ~ {end_default}) · 날짜는 자유롭게 변경할 수 있습니다.')

with st.form('search_form', clear_on_submit=False):
    st.caption('반납일 검색 방식')
    mode = st.radio('반납일 검색 방식', ['기간 검색', '하루 검색', '수기 입력(~)', '전체'],
                    index=0, horizontal=True, label_visibility='collapsed')
    c_date, c_vendor, c_product, c_search = st.columns([1.8, 1.25, 1.6, 0.65], vertical_alignment='bottom')
    start = end = day = manual = None
    with c_date:
        if mode == '기간 검색':
            dates = st.date_input('반납일 범위', (start_default, end_default), format='YYYY-MM-DD')
            if isinstance(dates, (tuple, list)) and len(dates) == 2:
                start, end = dates
        elif mode == '하루 검색':
            day = st.date_input('반납일', end_default, format='YYYY-MM-DD')
        elif mode == '수기 입력(~)':
            manual = st.text_input('반납일 범위', value=f'{start_default}~{end_default}', placeholder='2026-03-01~2026-03-20')
        else:
            st.text_input('반납일', value='전체 기간', disabled=True)
    with c_vendor:
        vendor = st.text_input('업체명', placeholder='예: 디엠케이')
    with c_product:
        product = st.text_input('상품명 / 내용', placeholder='예: 니트, 팬츠, 원피스')
    with c_search:
        searched = st.form_submit_button('검색', use_container_width=True, type='primary')

if searched:
    # Files added in Google Drive are discoverable directly from a search, no separate sync step.
    with st.spinner('새로 추가된 구글드라이브 파일을 확인하고 검색합니다...'):
        if can_connect:
            perform_sync()
        try:
            found = search_records(conn, mode, start=start, end=end, day=day, manual=manual or '', vendor=vendor, product=product)
            st.session_state.last_results = found
            st.session_state.search_error = None
        except ValueError as exc:
            st.session_state.last_results = None
            st.session_state.search_error = str(exc)
        st.session_state.searched = True

st.subheader('검색 결과')
if st.session_state.search_error:
    st.warning(st.session_state.search_error)
elif st.session_state.searched and st.session_state.last_results is not None:
    found = st.session_state.last_results
    if len(found):
        show = found[['반납일','업체명','주소','엑셀 기입 내용','원본파일']].copy()
        show['반납일'] = show['반납일'].dt.strftime('%Y-%m-%d')
        st.dataframe(show, use_container_width=True, hide_index=True)
        st.caption(f'검색 결과 {len(show):,}건 — 등록된 모든 내역은 반품 완료 기록입니다.')
    else:
        if st.session_state.sync_error or (st.session_state.sync_report and st.session_state.sync_report.failed):
            st.warning('현재 조건의 검색 결과가 없지만 일부 구글드라이브 파일이 동기화되지 않았으므로 반납 여부를 확정할 수 없습니다.')
        else:
            st.info('선택한 검색 조건과 일치하는 반품 완료 기록이 없습니다.')
        if mode != '전체' and (vendor.strip() or product.strip()):
            try:
                across = search_records(conn, '전체', vendor=vendor, product=product)
                if len(across):
                    st.caption(f'참고: 날짜 조건을 제외하면 일치하는 내역이 {len(across):,}건 있습니다. 기간을 변경하거나 「전체」를 선택하세요.')
            except Exception:
                pass
else:
    st.caption('조건을 입력하고 검색 버튼을 누르면 결과를 표시합니다.')

with st.expander('프로그램 안내 / 등록 현황 / 문제 파일'):
    st.write('**운영 방법**: Google Drive의 샘플관리파일 폴더(하위 폴더 포함)에 엑셀 또는 ZIP을 올리면 됩니다. 프로그램에 다시 업로드하거나 GitHub에 넣을 필요가 없습니다.')
    st.write('**자동 확인**: 앱을 새로 열 때와 검색 버튼을 누를 때 구글드라이브 파일을 확인합니다. 수동 동기화도 가능합니다.')
    st.write('**저장 방식**: Google Drive에 보관된 원본이 영구 자료입니다. SQLite는 Streamlit 서버에서 필요할 때 재생성되는 검색용 임시 인덱스입니다.')
    st.write('**날짜**: 파일명에 연도가 없는 MM-DD.xls는 2025년으로 읽고, YYYY-MM-DD.xls는 해당 연도로 읽습니다.')
    st.write('**검색 규칙**: 기간·업체명·상품명 조건은 모두 동시에 적용됩니다. 날짜 무관 검색은 「전체」로 바꾸세요.')
    st.write('**구글드라이브 직접 업로드**: 개인 「내 드라이브」에서는 서비스 계정에 파일 생성 소유권이 없어 웹 앱을 통한 직접 업로드가 실패할 수 있으므로 직원은 구글드라이브 화면에서 업로드하는 것을 권장합니다.')
    st.write(f'현재 검색 DB: {stats["rows"]:,}건 / 등록 파일 {stats["files"]:,}개 / Google Drive {stats["drive_files"]:,}개')
    if stats['date_min']:
        st.write(f'데이터 날짜 범위: {stats["date_min"]} ~ {stats["date_max"]}')
    if folder_id:
        st.write(f'설정된 Google Drive 폴더 ID: `{folder_id}`')
    errors = list(base_report.failed)
    if report:
        errors += list(report.failed)
    if errors:
        st.error('읽기 실패 / 확인 필요 파일:')
        st.code('\n'.join(errors[:100]))
    else:
        st.success('현재 동기화 단계에서 보고된 파일 읽기 오류가 없습니다.' if report else '아직 구글드라이브 동기화 결과가 없습니다.')

st.markdown("<div style='padding-top:28px; color:#737373; font-size:12px;'>copyright made by MISHARP COMPANY. MIYAWA. 2026</div>", unsafe_allow_html=True)
