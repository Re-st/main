"""
T4I-의회목록 스프레드시트를 활용하기 위한 유틸리티 함수
"""
import os
import gspread

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# 구글로부터 권한을 요청할 어플리케이션 목록
# 변경 시 token.json 삭제 후 재인증 필요
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
PWD = os.path.dirname(__file__)
BASE_DIR = os.path.join(PWD, os.pardir, os.pardir)


def google_authorization():
    """Google Sheets API 활용을 위한 인증 정보 요청
    credentials.json 파일을 토대로 인증을 요청하되, token.json 파일이 존재할 경우 거기에 저장된 정보 활용
    :todo: credentials.json 파일, token.json 파일 값을 환경변수로 설정
    :return: gspread.client.Client 인스턴스"""

    creds = None
    token_json_path = os.path.join(BASE_DIR, "_data", "token.json")
    # 이미 저장된 인증 정보가 있는지 확인
    if os.path.exists(token_json_path):
        creds = Credentials.from_authorized_user_file(token_json_path, SCOPES)

    # 인증 정보가 없거나 비정상적인 경우 인증 재요청
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(BASE_DIR, "_data", "credentials.json"), SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(token_json_path, "w") as token:
            token.write(creds.to_json())

    return gspread.authorize(creds)


def read_record_from_spreadsheet(
    link="https://docs.google.com/spreadsheets/d/1fBDJjkw8FSN5wXrvos9Q2wDsyItkUtNFGOxUZYE-h0M/edit#gid=1127955905",
) -> list[dict]:
    client = google_authorization()
    spreadsheet = client.open_by_url(link)
    worksheet = spreadsheet.get_worksheet(0)

    return worksheet.get_all_records()
