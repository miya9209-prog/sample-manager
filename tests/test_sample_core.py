import io
import sqlite3
import zipfile
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from sample_core import (
    FOLDER_MIME, SHEET_MIME, connect, default_dates, extract_date,
    get_stats, list_drive_files, parse_document, parse_excel, read_records,
    revision_hash, save_parsed, search_records, sync_drive
)


def make_workbook(lines, date_title='대구미즈 샘반 2026-03-17'):
    df = pd.DataFrame([[date_title, '', '', ''], ['', '거래처명', '주소', '내용']] +
                      [[str(i), v, addr, item] for i, (v, addr, item) in enumerate(lines, 1)])
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, header=False, index=False, sheet_name='Sheet1')
    return buffer.getvalue()


@pytest.fixture
def database(tmp_path):
    db = connect(tmp_path/'search.db')
    yield db
    db.close()


def test_date_filename_priority_and_legacy_year():
    assert extract_date('03-05.xls') == '2025-03-05'
    assert extract_date('12-08..xls') == '2025-12-08'
    assert extract_date('2026-03-31(2).xls') == '2026-03-31'
    assert extract_date('2026-05-12.xls') == '2026-05-12'


def test_parsing_vendor_product_and_date(database):
    blob=make_workbook([('디엠케이','디오트 3층','린넨 원피스 2'), ('까르르','청평 4층','맨투맨 3')])
    parsed=parse_excel(blob,'2026-03-17.xlsx')
    assert not parsed.failed_files
    assert len(parsed.rows)==2
    assert parsed.rows[0]['vendor']=='디엠케이'
    assert parsed.rows[0]['content']=='린넨 원피스 2'
    assert parsed.rows[0]['return_date']=='2026-03-17'
    save_parsed(database,'drive:id0',revision_hash(blob),'2026-03-17.xlsx',parsed)
    assert len(search_records(database,'전체',vendor='디 엠 케 이'))==1
    assert len(search_records(database,'전체',product='원피스'))==1
    assert len(search_records(database,'기간 검색',start=date(2026,3,1),end=date(2026,3,20),vendor='디엠케이'))==1
    assert len(search_records(database,'기간 검색',start=date(2025,3,1),end=date(2025,3,20),vendor='디엠케이'))==0
    assert len(search_records(database,'기간 검색',start=date(2026,3,18),end=date(2026,3,19),vendor='디엠케이'))==0
    assert len(search_records(database,'전체',vendor='디엠케이',product='맨투맨'))==0


def test_folder_recursive_file_types():
    class Req:
        def __init__(self,payload):self.payload=payload
        def execute(self):return self.payload
    class Files:
        def __init__(self):self.calls=[]
        def list(self,**kw):
            self.calls.append(kw)
            if "'root'" in kw['q']:
                return Req({'files':[{'id':'folder','name':'2026','mimeType':FOLDER_MIME},
                                     {'id':'xls','name':'2026-03-17.xls','mimeType':'application/vnd.ms-excel'},
                                     {'id':'gs','name':'반납내역','mimeType':SHEET_MIME}]})
            return Req({'files':[{'id':'inner','name':'2026-04-07.xlsx','mimeType':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'}]})
    class Service:
        def __init__(self):self.obj=Files()
        def files(self):return self.obj
    svc=Service()
    result=list_drive_files(svc,'root')
    assert {v['id'] for v in result} == {'xls','inner','gs'}
    assert all(x['supportsAllDrives'] and x['includeItemsFromAllDrives'] for x in svc.obj.calls)


def test_zip_and_retry_failed_then_updated(database):
    good=make_workbook([('디엠케이','1층','자켓')])
    z=io.BytesIO()
    with zipfile.ZipFile(z,'w') as archive:
        archive.writestr('반납/2026-05-12.xlsx',good)
    parsed=parse_document(z.getvalue(),'전체.zip')
    assert parsed.excel_files==1 and len(parsed.rows)==1
    assert parsed.rows[0]['return_date']=='2026-05-12'
    assert not parsed.failed_files
    with pytest.raises(ValueError):
        save_parsed(database,'drive:id1','bad','broken.xls',parse_document(b'bad','broken.xls'))
    assert database.execute("SELECT COUNT(*) FROM source_files WHERE source_id='drive:id1'").fetchone()[0]==0
    save_parsed(database,'drive:id1','v1','전체.zip',parsed)
    assert len(search_records(database,'전체',vendor='디엠케이'))==1
    newer=make_workbook([('디엠케이','1층','팬츠')])
    save_parsed(database,'drive:id1','v2','2026-05-12.xlsx',parse_excel(newer,'2026-05-12.xlsx'))
    assert len(search_records(database,'전체',product='자켓'))==0
    assert len(search_records(database,'전체',product='팬츠'))==1


def test_reboot_recovery_from_same_source(database,tmp_path):
    blob=make_workbook([('럭키룸','2층','데님 3')])
    parsed=parse_excel(blob,'2026-05-12.xlsx')
    save_parsed(database,'drive:abc','v1','2026-05-12.xlsx',parsed)
    db2=connect(tmp_path/'restarted.db')
    assert get_stats(db2)['rows']==0
    save_parsed(db2,'drive:abc','v1','2026-05-12.xlsx',parse_excel(blob,'2026-05-12.xlsx'))
    assert len(search_records(db2,'전체',vendor='럭키룸'))==1
    db2.close()


def test_default_latest_month(database):
    blob=make_workbook([('A상가','2층','티셔츠')])
    save_parsed(database,'repo:a','x','2026-05-12.xlsx',parse_excel(blob,'2026-05-12.xlsx'))
    start,end,label=default_dates(database,today=date(2026,10,25))
    assert start.isoformat() == '2026-05-01'
    assert end.isoformat() == '2026-05-12'
    assert label == '등록 데이터 최신 월'


class FakeRequest:
    def __init__(self, val):self.val=val
    def execute(self):return self.val
class FakeService:
    def __init__(self,files):self.files_value=files
    def files(self):return self
    def get(self,**kw):return FakeRequest({'id':kw['fileId'], 'mimeType':FOLDER_MIME})
    def list(self,**kw):return FakeRequest({'files':self.files_value})


def test_failed_file_not_marked_and_mock_drive(monkeypatch,database):
    import sample_core
    good=make_workbook([('디엠케이','2층','니트')])
    items=[{'id':'d1','name':'2026-03-05.xlsx','mimeType':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','modifiedTime':'2026-06-01'},
           {'id':'d2','name':'broken.xls','mimeType':'application/vnd.ms-excel','modifiedTime':'2026-06-01'}]
    svc=FakeService(items)
    def down(_,meta):return good if meta['id']=='d1' else b'not an excel'
    monkeypatch.setattr(sample_core,'download_drive_file',down)
    report=sync_drive(database,svc,'root')
    assert report.file_count==2 and report.new_files==1
    assert len(report.failed)==1 and 'broken.xls' in report.failed[0]
    assert get_stats(database)['drive_files']==1
    report2=sync_drive(database,svc,'root')
    assert report2.unchanged_files==1 and len(report2.failed)==1
