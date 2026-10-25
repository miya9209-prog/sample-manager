"""MISHARP sample return search engine. Original Drive files are source of truth.

SQLite is ONLY a reconstructible local search index on Streamlit Community Cloud.
No credentials or private keys belong in this repository.
"""
from __future__ import annotations

import hashlib
import io
import os
import re
import sqlite3
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

PARSER_VERSION = "2026-10-25-v2"
SUPPORTED_EXT = (".xls", ".xlsx", ".zip")
SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"
MAX_FILE = 45 * 1024 * 1024
MAX_ZIP_EXPANDED = 150 * 1024 * 1024
HEADERS_VENDOR = ("거래처명", "거래처", "업체명", "업체", "상호", "도매처명", "도매처", "상호명")
HEADERS_ADDRESS = ("주소", "위치", "상가위치", "매장위치")
HEADERS_CONTENT = ("내용", "상품명", "상품", "품목", "반납내용", "샘플명", "내역", "품명", "물품")
HEADERS_DATE = ("반납일", "반품일", "날짜", "일자", "반납날짜")
RESULT_COLUMNS = ["반납일", "업체명", "주소", "엑셀 기입 내용", "원본파일"]


@dataclass
class Parsed:
    rows: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    excel_files: int = 0
    failed_files: int = 0


@dataclass
class SyncReport:
    file_count: int = 0
    new_files: int = 0
    unchanged_files: int = 0
    row_count: int = 0
    failed: list[str] = field(default_factory=list)
    scanned_at: str = ""


def text_value(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (ValueError, TypeError):
        pass
    if isinstance(value, (datetime, pd.Timestamp)):
        return str(value.date())
    result = str(value).replace("\r", " ").replace("\n", " ").strip()
    if result.endswith(".0") and re.fullmatch(r"\d+\.0", result):
        result = result[:-2]
    return re.sub(r"\s+", " ", result)


def compact(value) -> str:
    return re.sub(r"\s+", "", text_value(value)).casefold()


def header_norm(value) -> str:
    return re.sub(r"[\s_\-:：/()]+", "", text_value(value)).lower()


def extract_date(source_name: str, raw: Optional[pd.DataFrame] = None) -> Optional[str]:
    """Explicit year in filename wins. Legacy MM-DD filename means 2025.

    A copied Excel template can contain a stale title/date, so filenames take priority.
    """
    name = Path(source_name.replace("\\", "/")).name
    m = re.search(r"(?<!\d)(20\d{2})[년._\-/\s]*(\d{1,2})[월._\-/\s]*(\d{1,2})(?:일)?(?!\d)", name)
    if not m:
        m = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", name)
    if m:
        try:
            return datetime(*map(int, m.groups())).date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(?<!\d)(\d{1,2})[-._](\d{1,2})(?!\d)", name)
    if m:
        try:
            return datetime(2025, *map(int, m.groups())).date().isoformat()
        except ValueError:
            pass
    if raw is not None:
        top = " ".join(text_value(v) for v in raw.iloc[:9, :12].values.flatten())
        for pat in (r"(?<!\d)(20\d{2})[년._\-/\s]+(\d{1,2})[월._\-/\s]+(\d{1,2})", r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)"):
            m = re.search(pat, top)
            if m:
                try:
                    return datetime(*map(int, m.groups())).date().isoformat()
                except ValueError:
                    continue
    return None


def is_heading(value, options) -> bool:
    norm = header_norm(value)
    if not norm:
        return False
    return norm in options or any(norm == f"{opt}(필수)" for opt in options)


def detect_header(raw: pd.DataFrame) -> tuple[Optional[int], dict[str, Optional[int]]]:
    best = (-1, None, {})
    for r in range(min(20, len(raw))):
        values = [header_norm(v) for v in raw.iloc[r].tolist()]
        mapped = {}
        for key, opts in (('vendor', HEADERS_VENDOR), ('address', HEADERS_ADDRESS), ('content', HEADERS_CONTENT), ('date', HEADERS_DATE)):
            exact = next((i for i, v in enumerate(values) if v in opts), None)
            if exact is None:
                exact = next((i for i, v in enumerate(values) if any(v.startswith(o) and len(v) <= len(o) + 4 for o in opts)), None)
            mapped[key] = exact
        score = (5 if mapped['vendor'] is not None else 0) + (3 if mapped['content'] is not None else 0) + (1 if mapped['address'] is not None else 0)
        if score > best[0]:
            best = score, r, mapped
    if best[0] >= 5:
        return best[1], best[2]
    return None, {}


def convert_row_date(val, fallback: Optional[str]) -> Optional[str]:
    if not text_value(val):
        return fallback
    if isinstance(val, (datetime, pd.Timestamp)):
        return val.date().isoformat()
    try:
        parsed = pd.to_datetime(val, errors='coerce')
        if pd.notna(parsed) and 2020 <= parsed.year <= 2100:
            return parsed.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        pass
    return fallback


def parse_excel(data: bytes, filename: str, display_name: Optional[str] = None) -> Parsed:
    output = Parsed(excel_files=1)
    if not filename.lower().endswith(('.xls', '.xlsx')):
        output.failed_files = 1
        output.warnings.append(f"{filename}: 엑셀 확장자가 아닙니다")
        return output
    if len(data) > MAX_FILE:
        output.failed_files = 1
        output.warnings.append(f"{filename}: 파일 크기가 제한을 초과했습니다")
        return output
    engine = 'xlrd' if filename.lower().endswith('.xls') else 'openpyxl'
    try:
        sheets = pd.read_excel(io.BytesIO(data), sheet_name=None, header=None, engine=engine, dtype=object)
    except Exception as exc:
        output.failed_files = 1
        output.warnings.append(f"{filename}: 파일 읽기 실패 ({type(exc).__name__}: {str(exc)[:120]})")
        return output
    any_data = False
    if not sheets:
        output.failed_files = 1
        output.warnings.append(f"{filename}: 시트가 없습니다")
        return output
    for sheet, raw in sheets.items():
        if raw.empty:
            continue
        header_row, mapping = detect_header(raw)
        if header_row is None:
            # MISHARP's legacy layout has: 1st row title, 2nd row headings;
            # skip sheets where neither vendor nor content can be identified.
            output.warnings.append(f"{filename} / {sheet}: 업체명 머리글을 찾지 못했습니다")
            continue
        fallback = extract_date(filename, raw)
        if not fallback and mapping.get('date') is None:
            output.warnings.append(f"{filename} / {sheet}: 반납일을 확인할 수 없습니다")
            continue
        start_rows = len(output.rows)
        for ridx in range(header_row + 1, len(raw)):
            row = raw.iloc[ridx]
            def get(key):
                idx = mapping.get(key)
                return row.iat[idx] if idx is not None else None
            vendor = text_value(get('vendor'))
            if vendor in ('합계', '소계', '총계'):
                continue
            address = text_value(get('address'))
            content = text_value(get('content'))
            # Often a shop has several product columns; keep ALL meaningful
            # item contents to avoid losing the product name in search.
            if not content and mapping.get('content') is None:
                used = {x for x in mapping.values() if x is not None}
                extras = [text_value(v) for i, v in enumerate(row.tolist()) if i not in used and text_value(v)]
                content = ', '.join(extras[:10])
            if not vendor and not content:
                continue
            return_date = convert_row_date(get('date'), fallback)
            if not return_date:
                output.warnings.append(f"{filename} / {sheet}: {ridx+1}행 날짜가 없습니다")
                continue
            if not vendor:
                # blank vendor may be a detail/continuation row; keep only if
                # content exists, for traceability rather than inventing a vendor.
                if not content:
                    continue
            output.rows.append({
                'return_date': return_date,
                'vendor': vendor,
                'address': address,
                'content': content,
                'source_file': display_name or filename,
                'source_entry': filename,
                'sheet_name': str(sheet),
                'excel_row': ridx + 1,
            })
        if len(output.rows) > start_rows:
            any_data = True
    if not any_data:
        output.failed_files = 1
        output.warnings.append(f"{filename}: 검색할 반납 내역이 추출되지 않았습니다")
    return output


def parse_document(data: bytes, name: str, mime_type: str = '') -> Parsed:
    if len(data) > MAX_FILE:
        return Parsed(warnings=[f"{name}: 파일 크기 제한 초과"], failed_files=1)
    if mime_type == SHEET_MIME:
        name = name if name.lower().endswith('.xlsx') else f"{name}.xlsx"
    if name.lower().endswith(('.xls', '.xlsx')):
        return parse_excel(data, name)
    if not name.lower().endswith('.zip'):
        return Parsed(warnings=[f"{name}: 지원하지 않는 형식"], failed_files=1)
    out = Parsed()
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            total_size = 0
            for member in archive.infolist():
                item_name = member.filename
                if member.is_dir() or Path(item_name).name.startswith(('~$', '.')):
                    continue
                if not item_name.lower().endswith(('.xls', '.xlsx')):
                    continue
                total_size += member.file_size
                if member.file_size > MAX_FILE or total_size > MAX_ZIP_EXPANDED:
                    raise ValueError('ZIP 압축 해제 크기 제한 초과')
                content = archive.read(member)
                parsed = parse_excel(content, Path(item_name).name, display_name=f"{name} / {item_name}")
                out.rows.extend(parsed.rows)
                out.warnings.extend(parsed.warnings)
                out.excel_files += parsed.excel_files
                out.failed_files += parsed.failed_files
    except Exception as exc:
        return Parsed(warnings=[f"{name}: ZIP 파일 읽기 실패 ({str(exc)[:120]})"], failed_files=1)
    if out.excel_files == 0:
        out.failed_files += 1
        out.warnings.append(f"{name}: ZIP에 XLS/XLSX 파일이 없습니다")
    return out


def connect(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(db_path), timeout=30)
    db.execute('PRAGMA busy_timeout = 30000')
    db.execute('PRAGMA journal_mode = WAL')
    db.execute('PRAGMA foreign_keys = ON')
    db.execute('CREATE TABLE IF NOT EXISTS source_files (source_id TEXT PRIMARY KEY, filename TEXT NOT NULL, revision TEXT NOT NULL, rows_count INTEGER NOT NULL, updated_at TEXT NOT NULL)')
    db.execute('''CREATE TABLE IF NOT EXISTS records (
        record_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, return_date TEXT NOT NULL,
        vendor TEXT NOT NULL, address TEXT NOT NULL, content TEXT NOT NULL,
        source_file TEXT NOT NULL, source_entry TEXT NOT NULL, sheet_name TEXT NOT NULL,
        excel_row INTEGER NOT NULL, FOREIGN KEY (source_id) REFERENCES source_files(source_id) ON DELETE CASCADE)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_return_date ON records(return_date)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_source_id ON records(source_id)')
    db.commit()
    return db


def revision_hash(data: bytes) -> str:
    return f"{PARSER_VERSION}:{hashlib.sha256(data).hexdigest()}"


def is_current(db, source_id: str, revision: str) -> bool:
    row = db.execute('SELECT revision FROM source_files WHERE source_id = ?', (source_id,)).fetchone()
    return bool(row and row[0] == revision)


def save_parsed(db, source_id: str, revision: str, filename: str, parsed: Parsed) -> int:
    if not parsed.rows or parsed.failed_files:
        raise ValueError('; '.join(parsed.warnings[:3]) or f'{filename}: 저장 가능한 데이터가 없습니다')
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    with db:
        # Source is replaced atomically only after full successful parsing.
        db.execute('INSERT INTO source_files (source_id, filename, revision, rows_count, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(source_id) DO UPDATE SET filename=excluded.filename, revision=excluded.revision, rows_count=excluded.rows_count, updated_at=excluded.updated_at', (source_id, filename, revision, len(parsed.rows), now))
        db.execute('DELETE FROM records WHERE source_id = ?', (source_id,))
        values = []
        for pos, row in enumerate(parsed.rows):
            uid = hashlib.sha256(f'{source_id}|{pos}|{row["source_entry"]}|{row["sheet_name"]}|{row["excel_row"]}'.encode()).hexdigest()
            values.append((uid, source_id, row['return_date'], row['vendor'], row['address'], row['content'], row['source_file'], row['source_entry'], row['sheet_name'], row['excel_row']))
        db.executemany('INSERT INTO records (record_id, source_id, return_date, vendor, address, content, source_file, source_entry, sheet_name, excel_row) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', values)
    return len(parsed.rows)


def seed_repository(db, data_dir: Path) -> SyncReport:
    summary = SyncReport(scanned_at=datetime.now().isoformat(timespec='seconds'))
    if not data_dir.exists():
        return summary
    for f in sorted(data_dir.rglob('*')):
        if not f.is_file() or f.name.startswith('~$') or f.suffix.lower() not in SUPPORTED_EXT:
            continue
        summary.file_count += 1
        source_id = 'repo:' + str(f.relative_to(data_dir).as_posix())
        try:
            contents = f.read_bytes()
            revision = revision_hash(contents)
            if is_current(db, source_id, revision):
                summary.unchanged_files += 1
                continue
            result = parse_document(contents, f.name)
            summary.row_count += save_parsed(db, source_id, revision, f.name, result)
            summary.new_files += 1
        except Exception as exc:
            summary.failed.append(f'{f.name}: {str(exc)[:180]}')
    return summary


def list_drive_files(service, folder_id: str, recursive=True) -> list[dict]:
    """List Excel/ZIP/Google Sheets files, including nested folders. All Drive types."""
    pending = [folder_id]
    seen = set()
    found = []
    while pending:
        parent = pending.pop(0)
        if parent in seen:
            continue
        seen.add(parent)
        page = None
        while True:
            # No MIME restriction: Google Sheets and new Office MIME types are supported.
            args = dict(q=f"'{parent}' in parents and trashed = false",
                        fields='nextPageToken, files(id,name,mimeType,md5Checksum,modifiedTime,size)',
                        pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True,
                        spaces='drive')
            if page:
                args['pageToken'] = page
            response = service.files().list(**args).execute()
            for item in response.get('files', []):
                if item.get('mimeType') == FOLDER_MIME:
                    if recursive:
                        pending.append(item['id'])
                elif not item.get('name', '').startswith('~$') and (item.get('mimeType') == SHEET_MIME or item.get('name', '').lower().endswith(SUPPORTED_EXT)):
                    found.append(item)
            page = response.get('nextPageToken')
            if not page:
                break
    return sorted(found, key=lambda f: (f.get('name', ''), f.get('id', '')))


def download_drive_file(service, meta: dict) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload
    if meta.get('mimeType') == SHEET_MIME:
        request = service.files().export_media(fileId=meta['id'], mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        request = service.files().get_media(fileId=meta['id'], supportsAllDrives=True)
    output = io.BytesIO()
    downloader = MediaIoBaseDownload(output, request)
    finished = False
    while not finished:
        _, finished = downloader.next_chunk()
        if output.tell() > MAX_FILE:
            raise ValueError('다운로드한 파일이 크기 제한을 초과했습니다')
    return output.getvalue()


def sync_drive(db, service, folder_id: str, progress: Optional[Callable[[int,int,str],None]] = None) -> SyncReport:
    # Check permissions early so a folder-id typo isn't misreported as no data.
    service.files().get(fileId=folder_id, fields='id,name,mimeType', supportsAllDrives=True).execute()
    files = list_drive_files(service, folder_id)
    result = SyncReport(file_count=len(files), scanned_at=datetime.now().isoformat(timespec='seconds'))
    for idx, meta in enumerate(files, start=1):
        label = meta.get('name', '이름 없음')
        if progress:
            progress(idx, len(files), label)
        source_id = 'drive:' + str(meta['id'])
        fast_revision = meta.get('md5Checksum') or f"{meta.get('modifiedTime', '')}:{meta.get('size', '')}"
        version = f'{PARSER_VERSION}:{fast_revision}'
        if is_current(db, source_id, version):
            result.unchanged_files += 1
            continue
        try:
            file_bytes = download_drive_file(service, meta)
            name = label
            parsed = parse_document(file_bytes, name, meta.get('mimeType', ''))
            inserted = save_parsed(db, source_id, version, label, parsed)
            result.row_count += inserted
            result.new_files += 1
        except Exception as exc:
            result.failed.append(f'{label}: {str(exc)[:220]}')
    return result


def read_records(db) -> pd.DataFrame:
    rows = pd.read_sql_query('''SELECT return_date AS 반납일, vendor AS 업체명,
      address AS 주소, content AS "엑셀 기입 내용", source_file AS 원본파일,
      source_id AS 소스ID, sheet_name AS 시트, excel_row AS 엑셀행
      FROM records ORDER BY return_date DESC, vendor ASC''', db)
    if not rows.empty:
        rows['반납일'] = pd.to_datetime(rows['반납일'], errors='coerce')
    return rows


def search_records(db, mode: str, start=None, end=None, day=None, manual='', vendor='', product='') -> pd.DataFrame:
    """AND-combine date/vendor/product; never skip date when vendor is set."""
    all_records = read_records(db)
    if all_records.empty:
        return all_records
    q = all_records.dropna(subset=['반납일'])
    if mode == '기간 검색':
        if start is None or end is None:
            raise ValueError('조회 시작일과 종료일을 모두 선택하세요.')
        if start > end:
            raise ValueError('조회 시작일이 종료일보다 늦습니다.')
        q = q[(q['반납일'] >= pd.Timestamp(start)) & (q['반납일'] <= pd.Timestamp(end))]
    elif mode == '하루 검색':
        if day is None:
            raise ValueError('반납일을 선택하세요.')
        q = q[q['반납일'] == pd.Timestamp(day)]
    elif mode == '수기 입력(~)':
        match = re.fullmatch(r'\s*(\d{4}-\d{1,2}-\d{1,2})\s*~\s*(\d{4}-\d{1,2}-\d{1,2})\s*', manual)
        if not match:
            raise ValueError('수기 기간은 2026-03-01~2026-03-20 형식으로 입력하세요.')
        try:
            s, e = (pd.Timestamp(v) for v in match.groups())
        except Exception:
            raise ValueError('올바른 날짜를 입력해주세요.')
        if s > e:
            raise ValueError('조회 시작일이 종료일보다 늦습니다.')
        q = q[(q['반납일'] >= s) & (q['반납일'] <= e)]
    v, p = compact(vendor), compact(product)
    if v:
        q = q[q['업체명'].map(compact).str.contains(v, regex=False, na=False)]
    if p:
        q = q[q['엑셀 기입 내용'].map(compact).str.contains(p, regex=False, na=False)]
    # GitHub may hold a second copy of files that are also present on Drive.
    # Prefer Drive in matching groups, but preserve repeated Excel rows and
    # distinct Drive files (which may represent separate actual returns).
    q = q.copy()
    keycols = ['반납일', '업체명', '주소', '엑셀 기입 내용']
    is_drive = q['소스ID'].str.startswith('drive:')
    groups = q.groupby(keycols, sort=False, dropna=False).ngroup()
    q['_group'] = groups
    q['_drive'] = is_drive
    group_has_drive = q.groupby('_group')['_drive'].transform('any')
    drive_records = q[q['_drive']]
    repo_records = q[~group_has_drive].copy()
    if not repo_records.empty:
        # If a file is copied twice into data/, show only the first copy.
        best_source = repo_records.groupby('_group')['소스ID'].transform('min')
        repo_records = repo_records[repo_records['소스ID'] == best_source]
    q = pd.concat([drive_records, repo_records], ignore_index=True)
    return q.sort_values(['반납일', '업체명', '원본파일'], ascending=[False, True, True]).drop(columns=['_group', '_drive']).reset_index(drop=True)


def default_dates(db, today=None):
    today = today or datetime.now().date()
    first = today.replace(day=1)
    latest = db.execute('SELECT MAX(return_date) FROM records').fetchone()[0]
    has_this_month = db.execute('SELECT 1 FROM records WHERE return_date >= ? AND return_date <= ? LIMIT 1', (first.isoformat(), today.isoformat())).fetchone()
    if has_this_month or not latest:
        return first, today, '이번 달'
    try:
        last = datetime.fromisoformat(latest).date()
    except ValueError:
        return first, today, '이번 달'
    return last.replace(day=1), last, '등록 데이터 최신 월'


def get_stats(db):
    total = db.execute('SELECT COUNT(*) FROM records').fetchone()[0]
    files = db.execute('SELECT COUNT(*) FROM source_files').fetchone()[0]
    drive_files = db.execute("SELECT COUNT(*) FROM source_files WHERE source_id LIKE 'drive:%'").fetchone()[0]
    date_min, date_max = db.execute('SELECT MIN(return_date), MAX(return_date) FROM records').fetchone()
    return dict(rows=total, files=files, drive_files=drive_files, date_min=date_min, date_max=date_max)
