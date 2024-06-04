from datetime import datetime, timedelta
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import requests

def send_slack_notification(message):
    webhook_url = 'https://hooks.slack.com/services/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'  # 실제 웹훅 URL로 변경하세요.
    payload = {'text': message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")

def download_data(ticker, start_date, end_date):
    try:
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            raise ValueError(f"No data found for {ticker}")
        return data
    except Exception as e:
        raise Exception(f"Failed to download data for {ticker}: {e}")

def calculate_median_and_mean(data, column, start_date, end_date):
    past_data = data.loc[start_date:end_date]
    return past_data[column].median(), past_data[column].mean()

def calculate_exchange_rate_history():
    start_date = datetime(2020, 1, 1)
    end_date = datetime.today()

    nikkei_data = download_data('^N225', start_date, end_date)
    krw_jpy_data = download_data('KRWJPY=X', start_date, end_date)

    results = []

    for date in nikkei_data.index:
        if date in krw_jpy_data.index:
            today_nikkei = nikkei_data.loc[date]['Close']
            today_krw_jpy = krw_jpy_data.loc[date]['Close']
            today_jpy_krw = 1 / today_krw_jpy

            if today_nikkei and today_krw_jpy:
                past_4_weeks = date - timedelta(weeks=4)
                
                nikkei_median, avg_nikkei = calculate_median_and_mean(nikkei_data, 'Close', past_4_weeks, date)
                krw_jpy_median, avg_krw_jpy = calculate_median_and_mean(krw_jpy_data, 'Close', past_4_weeks, date)
                jpy_krw_median = 1 / krw_jpy_median
                avg_jpy_krw = 1 / avg_krw_jpy

                nikkei_gap_ratio_new = (today_nikkei / jpy_krw_median) * 100
                avg_nikkei_gap_ratio = (nikkei_data.loc[past_4_weeks:date]['Close'] / (1 / krw_jpy_data.loc[past_4_weeks:date]['Close'])).mean() * 100
                jpy_krw_estimate = (today_nikkei / avg_nikkei_gap_ratio) * 100

                conditions = [
                    today_jpy_krw < avg_jpy_krw,
                    today_nikkei < avg_nikkei,
                    nikkei_gap_ratio_new > avg_nikkei_gap_ratio,
                    today_jpy_krw < jpy_krw_estimate
                ]
                suitability = ['적합' if condition else '부적합' for condition in conditions]
                final_suitability = '적합' if all(conditions) else '부적합'
                
                profit_loss_results = []
                if final_suitability == '적합':
                    for days_ahead in range(1, 15):
                        future_date = date + timedelta(days=days_ahead)
                        if future_date in krw_jpy_data.index:
                            future_krw_jpy = krw_jpy_data.loc[future_date]['Close']
                            future_jpy_krw = 1 / future_krw_jpy
                            profit_loss_percentage = ((future_jpy_krw - today_jpy_krw) / today_jpy_krw) * 100
                            profit_loss_results.append(f"{profit_loss_percentage:.2f}%")
                        else:
                            profit_loss_results.append('N/A')
                else:
                    profit_loss_results = ['' for _ in range(1, 15)]

                result = [
                    date.strftime('%Y-%m-%d'),
                    f'{jpy_krw_estimate:.4f} KRW',
                    f'{nikkei_gap_ratio_new:.2f}%',
                    round(today_nikkei, 2),
                    round(today_jpy_krw, 4),
                    f'{avg_nikkei_gap_ratio:.2f}%',
                    round(avg_nikkei, 2),
                    round(avg_jpy_krw, 4),
                    *suitability,
                    final_suitability,
                    *profit_loss_results
                ]

                results.append(result)

    record_to_sheet(results)

def record_to_sheet(data):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'), scope)
    client = gspread.authorize(creds)

    sheet = client.open("적정환율알림서비스").worksheet("엔_4주_백테스트")

    sheet.clear()

    titles = [
        '현재날짜', '적정엔화환율', '현재Nikkei갭비율', '현재Nikkei지수', '현재JPY/KRW환율',
        '평균Nikkei갭비율', '평균Nikkei지수', '평균JPY/KRW환율', '적합조건1', '적합조건2', '적합조건3',
        '적합조건4', '전체조건적합성여부', '1일후 예상수익', '2일후 예상수익', '3일후 예상수익',
        '4일후 예상수익', '5일후 예상수익', '6일후 예상수익', '7일후 예상수익', '8일후 예상수익',
        '9일후 예상수익', '10일후 예상수익', '11일후 예상수익', '12일후 예상수익', '13일후 예상수익',
        '14일후 예상수익'
    ]
    sheet.append_row(titles)

    sheet.append_rows(data)

# 과거 데이터에 대한 적정환율 계산 및 기록
calculate_exchange_rate_history()
