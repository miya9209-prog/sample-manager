# sample-manager

미샵 샘플 반품 관리 프로그램입니다.

## 운영 방식

1. Google Drive에 샘플반납 엑셀/ZIP 파일을 업로드합니다.
2. Streamlit 앱에서 `구글드라이브 동기화` 버튼을 누릅니다.
3. 프로그램이 파일을 읽어 `sample_returns.db`에 검색용 데이터로 누적 저장합니다.
4. 검색은 DB 기준으로 실행됩니다.

## Streamlit Secrets

Streamlit Cloud → App → Settings → Secrets에 아래 형식으로 입력합니다.

```toml
[gdrive]
folder_id = "구글드라이브_폴더_ID"
service_account_json = """
서비스계정_JSON_전체내용
"""
```

서비스계정 이메일을 Google Drive 폴더에 `뷰어` 또는 `편집자` 권한으로 공유해야 합니다.
