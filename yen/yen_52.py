import os
from datetime import datetime, timedelta
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests

def send_slack_notification(message):
    """
    슬랙에 알림을 보내는 함수
    :param message: str, 슬랙으로 보낼 메시지
    """
    webhook_url = 'https://hooks.slack.com/services/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'  # 여기에 실제 웹훅 URL을 넣으세요.
    payload = {'text': message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")

def download_data(period_weeks):
    """
    지정된 기간 동안의 Nikkei 225와 KRW/JPY 환율 데이터를 다운로드하는 함수
    :param period_weeks: int, 데이터를 다운로드할 기간(주)
    :return: tuple, Nikkei 225 데이터프레임과 KRW/JPY 데이터프레임
    """
    start = datetime.today() - timedelta(weeks=period_weeks)  # 시작 날짜
    end = datetime.today()  # 종료 날짜
    nikkei_data = yf.download('^N225', start=start, end=end)  # Nikkei 225 데이터 다운로드
    krw_jpy_data = yf.download('KRWJPY=X', start=start, end=end)  # KRW/JPY 환율 데이터 다운로드
    return nikkei_data, krw_jpy_data

def calculate_indicators(nikkei_data, krw_jpy_data):
    """
    다양한 금융 지표를 계산하는 함수
    :param nikkei_data: DataFrame, Nikkei 225 데이터
    :param krw_jpy_data: DataFrame, KRW/JPY 환율 데이터
    :return: dict, 계산된 지표들을 포함하는 딕셔너리
    """
    today_nikkei = round(nikkei_data['Close'].iloc[-1], 2)  # 현재 Nikkei 225 지수
    today_krw_jpy = round(krw_jpy_data['Close'].iloc[-1], 4)  # 현재 KRW/JPY 환율 (소수점 4자리까지)
    today_jpy_krw = round(1 / today_krw_jpy, 4)  # 현재 JPY/KRW 환율 (소수점 4자리까지)
    nikkei_median = round(nikkei_data['Close'].median(), 2)  # Nikkei 225 중앙값
    krw_jpy_median = round(krw_jpy_data['Close'].median(), 4)  # KRW/JPY 환율 중앙값
    jpy_krw_median = round(1 / krw_jpy_median, 4)  # JPY/KRW 환율 중앙값
    nikkei_gap_ratio = round((today_nikkei / nikkei_median) * 100, 2)  # Nikkei 격차 비율
    avg_nikkei_gap_ratio = round((nikkei_data['Close'] / (1 / krw_jpy_data['Close'])).mean() * 100, 2)  # 평균 Nikkei 갭 비율
    avg_nikkei = round(nikkei_data['Close'].mean(), 2)  # 평균 Nikkei 225 지수
    avg_krw_jpy = round(krw_jpy_data['Close'].mean(), 4)  # 평균 KRW/JPY 환율
    avg_jpy_krw = round(1 / avg_krw_jpy, 4)  # 평균 JPY/KRW 환율
    jpy_krw_estimate = round((today_nikkei / avg_nikkei_gap_ratio) * 100, 4)  # 적정 JPY/KRW 환율
    nikkei_gap_percentage = round(((today_nikkei - nikkei_median) / nikkei_median) * 100, 1)  # Nikkei 격차 퍼센트
    nikkei_gap_ratio_new = round((today_nikkei / jpy_krw_median) * 100, 2)  # 새로운 Nikkei 격차 비율
    return {
        'today_nikkei': today_nikkei,
        'today_jpy_krw': today_jpy_krw,
        'nikkei_median': nikkei_median,
        'jpy_krw_median': jpy_krw_median,
        'nikkei_gap_ratio': nikkei_gap_ratio,
        'avg_nikkei_gap_ratio': avg_nikkei_gap_ratio,
        'avg_nikkei': avg_nikkei,
        'avg_jpy_krw': avg_jpy_krw,
        'jpy_krw_estimate': jpy_krw_estimate,
        'nikkei_gap_percentage': nikkei_gap_percentage,
        'nikkei_gap_ratio_new': nikkei_gap_ratio_new
    }

def check_conditions(indicators):
    """
    투자 적합성 조건을 확인하는 함수
    :param indicators: dict, 계산된 지표들을 포함하는 딕셔너리
    :return: tuple, 각 조건의 만족 여부를 나타내는 불리언 값들의 튜플
    """
    condition1 = indicators['today_jpy_krw'] < indicators['avg_jpy_krw']  # 조건 1: 현재 JPY/KRW 환율이 평균 JPY/KRW 환율보다 낮은가
    condition2 = indicators['today_nikkei'] < indicators['avg_nikkei']  # 조건 2: 현재 Nikkei 225 지수가 평균 Nikkei 225 지수보다 낮은가
    condition3 = indicators['nikkei_gap_ratio_new'] > indicators['avg_nikkei_gap_ratio']  # 조건 3: 새로운 Nikkei 격차 비율이 평균 Nikkei 격차 비율보다 높은가
    condition4 = indicators['today_jpy_krw'] < indicators['jpy_krw_estimate']  # 조건 4: 현재 JPY/KRW 환율이 적정 JPY/KRW 환율보다 낮은가
    return condition1, condition2, condition3, condition4

def provide_advice(suitable_conditions):
    """
    적합한 조건의 수에 따라 투자 조언을 제공하는 함수
    :param suitable_conditions: int, 적합한 조건의 수
    :return: str, 투자 조언
    """
    if suitable_conditions == 4:
        return '52주 - 지금 바로 투자하세요'
    elif suitable_conditions >= 3:
        return '52주 - 투자 시작해도 됨'
    else:
        return '52주 - 투자 보류'

def create_result(period_weeks, indicators, conditions, advice, final_suitability):
    """
    최종 결과를 생성하는 함수
    :param period_weeks: int, 데이터를 분석한 기간(주)
    :param indicators: dict, 계산된 지표들을 포함하는 딕셔너리
    :param conditions: tuple, 각 조건의 만족 여부를 나타내는 불리언 값들의 튜플
    :param advice: str, 투자 조언
    :param final_suitability: str, 최종 의견
    :return: dict, 최종 결과를 포함하는 딕셔너리
    """
    # Nikkei 격차 퍼센트에 따른 조건문
    if indicators['nikkei_gap_percentage'] > 5:
        trading_advice = '엔화 단타 - 매도하세요!'
    elif indicators['nikkei_gap_percentage'] <= -5:
        trading_advice = '엔화 단타 - 매수하세요!'
    else:
        trading_advice = '엔화 단타 없어요!'
    
    return {
        '현재 날짜': datetime.today().strftime('%Y-%m-%d %H:%M'),
        '기간': f'{period_weeks}주',
        '적정 엔화환율': f'{indicators["jpy_krw_estimate"]} 원',
        '현재 Nikkei 갭 비율': f'{indicators["nikkei_gap_ratio_new"]}%',
        '현재 Nikkei 225 지수': indicators['today_nikkei'],
        '현재 JPY/KRW 환율': indicators['today_jpy_krw'],
        f'{period_weeks}주 평균 Nikkei 갭 비율': f'{indicators["avg_nikkei_gap_ratio"]}%',
        f'{period_weeks}주 평균 Nikkei 225 지수': indicators['avg_nikkei'],
        f'{period_weeks}주 평균 JPY/KRW 환율': indicators['avg_jpy_krw'],
        '조건1 (현재 JPY/KRW 환율 < 52주 평균 환율)': '적합' if conditions[0] else '부적합',
        '조건2 (현재 Nikkei 225 지수 < 52주 평균 Nikkei 225 지수)': '적합' if conditions[1] else '부적합',
        '조건3 (현재 Nikkei 갭 비율 > 52주 평균 Nikkei 갭 비율)': '적합' if conditions[2] else '부적합',
        '조건4 (현재 JPY/KRW 환율 < 적정 환율)': '적합' if conditions[3] else '부적합',
        '전체조건접합성여부': final_suitability,
        '투자매수매도 조언': trading_advice
    }

def record_to_sheet(data):
    """
    결과를 구글 스프레드시트에 기록하는 함수
    :param data: dict, 기록할 최종 결과를 포함하는 딕셔너리
    """
    # 구글 스프레드시트 API 인증 및 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'), scope)
    client = gspread.authorize(creds)
    
    # 스프레드시트 열기 및 시트 선택
    sheet = client.open("적정환율알림서비스").worksheet("엔_52주")
    
    # 데이터 기록
    row = [data['현재 날짜'], data['기간']] + list(data.values())[2:]
    sheet.append_row(row)

def calculate_exchange_rate(period_weeks):
    """
    주요 로직을 실행하는 함수
    :param period_weeks: int, 데이터를 분석할 기간(주)
    """
    # 데이터 다운로드
    nikkei_data, krw_jpy_data = download_data(period_weeks)
    
    # 지표 계산
    indicators = calculate_indicators(nikkei_data, krw_jpy_data)
    
    # 조건 확인
    conditions = check_conditions(indicators)
    suitable_conditions = sum(conditions)
    
    # 조언 제공
    advice = provide_advice(suitable_conditions)
    final_suitability = advice
    
    # 결과 생성
    result = create_result(period_weeks, indicators, conditions, advice, final_suitability)
    
    # 결과 출력
    for key, value in result.items():
        print(f'{key}: {value}')
    print('\n' + '-'*50 + '\n')
    
    # 스프레드시트에 기록
    record_to_sheet(result)
    
    # 슬랙 알림 전송
    if suitable_conditions >= 3 or final_suitability == '지금 바로 투자하세요':
        send_slack_notification(f"{advice}: {final_suitability}")

# 52주 기준으로 데이터 계산
calculate_exchange_rate(52)
