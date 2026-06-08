# 미샵 샘플 반품 관리 프로그램 - Google Drive 연동 설정

## 1. 구글드라이브 폴더 ID 확인
구글드라이브에서 `샘플관리파일` 폴더를 열면 주소가 아래처럼 보입니다.

```text
https://drive.google.com/drive/folders/폴더ID
```

`folders/` 뒤의 긴 문자열이 `GDRIVE_FOLDER_ID`입니다.

## 2. Streamlit Secrets 설정
Streamlit Cloud → sample-manager 앱 → Settings → Secrets에 `.streamlit/secrets.toml.example` 형식으로 입력합니다.

필수값:

```toml
GDRIVE_FOLDER_ID = "구글드라이브_폴더_ID"

gcp_service_account_json = '''
JSON 키 파일 전체 내용
'''
```

## 3. 구글드라이브 폴더 공유
`샘플관리파일` 폴더를 아래 서비스 계정 이메일에 공유합니다.

```text
sample-manager@misharp-sample-manager.iam.gserviceaccount.com
```

권한은 `편집자`로 설정합니다.

## 4. 직원 사용 방법
1. 구글드라이브 `샘플관리파일` 폴더에 XLS/XLSX/ZIP 업로드
2. 프로그램에서 `구글드라이브 동기화` 클릭
3. 검색

프로그램 화면의 파일 업로드창을 사용해도 구글드라이브에 저장됩니다.
