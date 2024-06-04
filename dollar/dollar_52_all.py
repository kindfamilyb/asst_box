from datetime import datetime, timedelta  # 날짜와 시간 계산을 위한 모듈 임포트
import yfinance as yf  # 금융 데이터를 다운로드하기 위한 yfinance 모듈 임포트
import gspread  # 구글 스프레드시트와 연동하기 위한 gspread 모듈 임포트
from oauth2client.service_account import ServiceAccountCredentials  # 구글 서비스 계정 인증을 위한 모듈 임포트
import os  # 환경 변수 접근을 위한 os 모듈 임포트

def download_data(ticker, start_date, end_date):  # 데이터를 다운로드하는 함수 정의
    """Download historical data for a given ticker from Yahoo Finance."""  # 함수 설명
    try:
        data = yf.download(ticker, start=start_date, end=end_date)  # 주어진 티커의 데이터를 다운로드
        if data.empty:
            raise ValueError(f"No data found for {ticker}")  # 데이터가 없으면 예외 발생
        return data  # 데이터를 반환
    except Exception as e:
        raise Exception(f"Failed to download data for {ticker}: {e}")  # 다운로드 실패 시 예외 발생

def calculate_median_and_mean(data, column, start_date, end_date):  # 중앙값과 평균을 계산하는 함수 정의
    """Calculate the median and mean for a specific column over a date range."""  # 함수 설명
    past_data = data.loc[start_date:end_date]  # 주어진 날짜 범위의 데이터 선택
    return past_data[column].median(), past_data[column].mean()  # 중앙값과 평균 반환

def calculate_exchange_rate_history():  # 환율 히스토리를 계산하는 함수 정의
    # 데이터 수집을 위한 시작 날짜와 종료 날짜 설정
    start_date = datetime(2020, 1, 1)  # 시작 날짜
    end_date = datetime.today()  # 종료 날짜

    # USD 인덱스와 USD/KRW 환율 데이터 다운로드
    usd_index_data = download_data('DX-Y.NYB', start_date, end_date)  # USD 인덱스 데이터 다운로드
    usd_krw_data = download_data('USDKRW=X', start_date, end_date)  # USD/KRW 환율 데이터 다운로드

    results = []  # 결과를 저장할 리스트 초기화

    for date in usd_index_data.index:  # USD 인덱스 데이터의 각 날짜에 대해 반복
        if date in usd_krw_data.index:  # 해당 날짜가 USD/KRW 데이터에도 존재하는 경우
            today_usd_index = usd_index_data.loc[date]['Close']  # 해당 날짜의 USD 인덱스 값
            today_usd_krw = usd_krw_data.loc[date]['Close']  # 해당 날짜의 USD/KRW 환율 값

            if today_usd_index and today_usd_krw:  # 데이터가 존재하는 경우
                past_52_weeks = date - timedelta(weeks=52)  # 과거 52주 기간 설정
                
                # 과거 4주간의 중앙값과 평균 계산
                usd_index_median, avg_usd_index = calculate_median_and_mean(usd_index_data, 'Close', past_52_weeks, date)  # USD 인덱스 중앙값과 평균
                usd_krw_median, avg_usd_krw = calculate_median_and_mean(usd_krw_data, 'Close', past_52_weeks, date)  # USD/KRW 환율 중앙값과 평균
                
                # 갭 비율과 적정 환율 계산
                usd_gap_ratio_new = (today_usd_index / usd_krw_median) * 100  # 현재 USD 갭 비율
                avg_usd_gap_ratio = (usd_index_data.loc[past_52_weeks:date]['Close'] / usd_krw_data.loc[past_52_weeks:date]['Close']).mean() * 100  # 평균 USD 갭 비율
                usd_krw_estimate = (today_usd_index / avg_usd_gap_ratio) * 100  # 적정 USD/KRW 환율

                # 적합성 판단
                conditions = [
                    today_usd_krw < avg_usd_krw,  # 조건 1: 현재 USD/KRW 환율 < 평균 USD/KRW 환율
                    today_usd_index < avg_usd_index,  # 조건 2: 현재 USD 인덱스 < 평균 USD 인덱스
                    usd_gap_ratio_new > avg_usd_gap_ratio,  # 조건 3: 현재 USD 갭 비율 > 평균 USD 갭 비율
                    today_usd_krw < usd_krw_estimate  # 조건 4: 현재 USD/KRW 환율 < 적정 USD/KRW 환율
                ]
                suitability = ['적합' if condition else '부적합' for condition in conditions]  # 각 조건에 대한 적합성 판단
                final_suitability = '적합' if all(conditions) else '부적합'  # 모든 조건이 만족되면 '적합', 아니면 '부적합'
                
                profit_loss_results = []
                if final_suitability == '적합':  # 전체 조건 적합성이 '적합'인 경우에만 계산
                    for days_ahead in range(1, 15):  # 1일부터 14일까지의 예상 수익률 계산
                        future_date = date + timedelta(days=days_ahead)  # 미래 날짜 계산
                        if future_date in usd_krw_data.index:  # 미래 날짜가 데이터에 존재하는 경우
                            future_usd_krw = usd_krw_data.loc[future_date]['Close']  # 미래 날짜의 USD/KRW 환율 값
                            profit_loss_percentage = ((future_usd_krw - today_usd_krw) / today_usd_krw) * 100  # 예상 수익률 계산
                            profit_loss_results.append(f"{profit_loss_percentage:.2f}%")  # 예상 수익률 포맷팅
                        else:
                            profit_loss_results.append('N/A')  # 데이터가 없는 경우 'N/A'
                else:
                    profit_loss_results = ['' for _ in range(1, 15)]  # 적합하지 않은 경우 빈 값

                result = [
                    date.strftime('%Y-%m-%d'),  # 현재 날짜
                    f'{usd_krw_estimate:.2f} 원',  # 적정 USD/KRW 환율
                    f'{usd_gap_ratio_new:.2f}%',  # 현재 USD 갭 비율
                    round(today_usd_index, 2),  # 현재 USD 인덱스
                    round(today_usd_krw, 2),  # 현재 USD/KRW 환율
                    f'{avg_usd_gap_ratio:.2f}%',  # 평균 USD 갭 비율
                    round(avg_usd_index, 2),  # 평균 USD 인덱스
                    round(avg_usd_krw, 2),  # 평균 USD/KRW 환율
                    *suitability,  # 적합성 판단 결과
                    final_suitability,  # 전체 조건 적합성 여부
                    *profit_loss_results  # 1일부터 14일까지 예상 수익률
                ]

                results.append(result)  # 결과 리스트에 추가

    record_to_sheet(results)  # 결과를 스프레드시트에 기록

def record_to_sheet(data):  # 데이터를 스프레드시트에 기록하는 함수 정의
    # 구글 스프레드시트 API 인증 및 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]  # 인증 범위 설정
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'), scope)  # 서비스 계정 인증 정보 로드
    client = gspread.authorize(creds)  # 인증된 클라이언트 생성

    # 스프레드시트 열기 및 시트 선택
    sheet = client.open("적정환율알림서비스").worksheet("52주기준달러전체기간적정환율예상수익")  # 스프레드시트와 시트 열기

    # 기존 데이터 삭제
    sheet.clear()  # 기존 데이터 삭제

    # 타이틀 행 추가
    titles = [
        '현재날짜', '적정원달러', '현재달러갭비율', '현재달러인덱스', '현재원달러환율',
        '평균달러갭비율', '평균달러인덱스', '평균환율', '적합조건1', '적합조건2', '적합조건3',
        '적합조건4', '전체조건적합성여부', '1일후 예상수익', '2일후 예상수익', '3일후 예상수익',
        '4일후 예상수익', '5일후 예상수익', '6일후 예상수익', '7일후 예상수익', '8일후 예상수익',
        '9일후 예상수익', '10일후 예상수익', '11일후 예상수익', '12일후 예상수익', '13일후 예상수익',
        '14일후 예상수익'
    ]
    sheet.append_row(titles)  # 타이틀 행 추가

    # 데이터 기록 (일괄 처리)
    sheet.append_rows(data)  # 데이터를 일괄 추가

# 과거 데이터에 대한 적정환율 계산 및 기록
calculate_exchange_rate_history()  # 환율 히스토리 계산 및 기록 함수 호출
