# 보조지표
# 금은 비율
# 원달러환율
# 달러지수
# 달러갭비율
# 적정환율
# 적정금가격
# 예상수익률

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
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')  # 환경 변수에서 웹훅 URL을 가져옵니다.
    payload = {'text': message}
    response = requests.post(webhook_url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")

def download_data(period_weeks):
    """
    지정된 기간 동안의 USD Index, USD/KRW 환율, 금 가격, 은 가격 데이터를 다운로드하는 함수
    :param period_weeks: int, 데이터를 다운로드할 기간(주)
    :return: tuple, USD Index 데이터프레임, USD/KRW 데이터프레임, 금 가격 데이터프레임, 은 가격 데이터프레임
    """
    start = datetime.today() - timedelta(weeks=period_weeks)  # 시작 날짜
    end = datetime.today()  # 종료 날짜
    usd_index_data = yf.download('DX-Y.NYB', start=start, end=end)  # USD Index 데이터 다운로드
    usd_krw_data = yf.download('USDKRW=X', start=start, end=end)  # USD/KRW 환율 데이터 다운로드
    gold_data = yf.download('GC=F', start=start, end=end)  # 금 가격 데이터 다운로드
    silver_data = yf.download('SI=F', start=start, end=end)  # 은 가격 데이터 다운로드
    return usd_index_data, usd_krw_data, gold_data, silver_data

def calculate_indicators(usd_index_data, usd_krw_data, gold_data, silver_data):
    """
    다양한 금융 지표를 계산하는 함수
    :param usd_index_data: DataFrame, USD Index 데이터
    :param usd_krw_data: DataFrame, USD/KRW 환율 데이터
    :param gold_data: DataFrame, 금 가격 데이터
    :param silver_data: DataFrame, 은 가격 데이터
    :return: dict, 계산된 지표들을 포함하는 딕셔너리
    """
    today_usd_index = round(usd_index_data['Close'].iloc[-1], 2)  # 현재 달러 인덱스
    today_usd_krw = round(usd_krw_data['Close'].iloc[-1], 2)  # 현재 USD/KRW 환율
    today_gold = round(gold_data['Close'].iloc[-1], 2)  # 현재 금 가격 (USD/온스)
    today_silver = round(silver_data['Close'].iloc[-1], 2)  # 현재 은 가격 (USD/온스)
    gold_silver_ratio = round(today_gold / today_silver, 2)  # 금/은 비율

    # 온스를 g으로 변환 (1 온스 = 31.1035 g)
    today_gold_g = round(today_gold / 31.1035, 2)  # 현재 금 가격 (USD/g)
    gold_price_krw_g = round(today_gold_g * today_usd_krw, 2)  # 현재 금 가격 (KRW/g)

    usd_index_median = round(usd_index_data['Close'].median(), 2)  # USD 인덱스 중앙값
    usd_krw_median = round(usd_krw_data['Close'].median(), 2)  # USD/KRW 환율 중앙값
    usd_gap_ratio = round((today_usd_index / usd_index_median) * 100, 2)  # 달러 격차 비율
    avg_usd_gap_ratio = round((usd_index_data['Close'] / usd_krw_data['Close']).mean() * 100, 2)  # 평균 달러 갭 비율
    avg_usd_index = round(usd_index_data['Close'].mean(), 2)  # 평균 USD 인덱스
    avg_usd_krw = round(usd_krw_data['Close'].mean(), 2)  # 평균 USD/KRW 환율
    usd_krw_estimate = round((today_usd_index / avg_usd_gap_ratio) * 100, 2)  # 적정 USD/KRW 환율
    usd_gap_persentage = round(((today_usd_index - usd_index_median) / usd_index_median) * 100, 1)  # USD 격차 퍼센트
    usd_gap_ratio_new = round((today_usd_index / usd_krw_median) * 100, 2)  # 새로운 달러 격차 비율

    avg_gold = round(gold_data['Close'].mean(), 2)  # 평균 금 가격 (USD/온스)
    gold_estimate = round((today_usd_index / avg_usd_gap_ratio) * avg_gold, 2)  # 적정 금 가격 (USD/온스)
    gold_estimate_g = round(gold_estimate / 31.1035, 2)  # 적정 금 가격 (USD/g)
    gold_estimate_krw_g = round(gold_estimate_g * today_usd_krw, 2)  # 적정 금 가격 (KRW/g)
    expected_return = round(((gold_estimate_krw_g - gold_price_krw_g) / gold_price_krw_g) * 100, 2)  # 예상 수익률

    return {
        'today_usd_index': today_usd_index,
        'today_usd_krw': today_usd_krw,
        'today_gold': today_gold,
        'today_silver': today_silver,
        'gold_silver_ratio': gold_silver_ratio,
        'usd_index_median': usd_index_median,
        'usd_krw_median': usd_krw_median,
        'usd_gap_ratio': usd_gap_ratio,
        'avg_usd_gap_ratio': avg_usd_gap_ratio,
        'avg_usd_index': avg_usd_index,
        'avg_usd_krw': avg_usd_krw,
        'usd_krw_estimate': usd_krw_estimate,
        'usd_gap_persentage': usd_gap_persentage,
        'usd_gap_ratio_new': usd_gap_ratio_new,
        'gold_estimate': gold_estimate,
        'gold_price_krw_g': gold_price_krw_g,
        'gold_estimate_krw_g': gold_estimate_krw_g,
        'expected_return': expected_return
    }

def check_conditions(indicators):
    """
    투자 적합성 조건을 확인하는 함수
    :param indicators: dict, 계산된 지표들을 포함하는 딕셔너리
    :return: tuple, 각 조건의 만족 여부를 나타내는 불리언 값들의 튜플
    """
    condition1 = indicators['today_usd_krw'] < indicators['avg_usd_krw']  # 조건 1: 현재 USD/KRW 환율이 평균 USD/KRW 환율보다 낮은가
    condition2 = indicators['today_usd_index'] < indicators['avg_usd_index']  # 조건 2: 현재 달러 인덱스가 평균 달러 인덱스보다 낮은가
    condition3 = indicators['usd_gap_ratio_new'] > indicators['avg_usd_gap_ratio']  # 조건 3: 새로운 달러 격차 비율이 평균 달러 격차 비율보다 높은가
    condition4 = indicators['today_usd_krw'] < indicators['usd_krw_estimate']  # 조건 4: 현재 USD/KRW 환율이 적정 USD/KRW 환율보다 낮은가
    condition5 = indicators['gold_silver_ratio'] > 80  # 조건 5: 금/은 비율이 80보다 높은가
    return condition1, condition2, condition3, condition4, condition5

def provide_advice(suitable_conditions):
    """
    적합한 조건의 수에 따라 투자 조언을 제공하는 함수
    :param suitable_conditions: int, 적합한 조건의 수
    :return: str, 투자 조언
    """
    if suitable_conditions == 5:
        return '지금 바로 투자하세요'
    elif suitable_conditions >= 3:
        return '투자 시작해도 됨'
    else:
        return '투자 보류'

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
    return {
        '현재 날짜': datetime.today().strftime('%Y-%m-%d %H:%M'),
        '기간': f'{period_weeks}주',
        '적정 금 가격 (KRW/g)': f'{indicators["gold_estimate_krw_g"]} 원',
        '예상 수익률': f'{indicators["expected_return"]}%',
        '현재 USD 지수': indicators['today_usd_index'],
        '현재 USD/KRW 환율': indicators['today_usd_krw'],
        '현재 금 가격 (USD/온스)': f'{indicators["today_gold"]} USD',
        '현재 금 가격 (KRW/g)': f'{indicators["gold_price_krw_g"]} 원',
        '현재 은 가격 (USD/온스)': f'{indicators["today_silver"]} USD',
        '금/은 비율': f'{indicators["gold_silver_ratio"]}',
        f'{period_weeks}주 평균 달러 갭 비율': f'{indicators["avg_usd_gap_ratio"]}%',
        f'{period_weeks}주 평균 USD 지수': indicators['avg_usd_index'],
        f'{period_weeks}주 평균 USD/KRW 환율': indicators['avg_usd_krw'],
        '조건1 (현재 원달러 환율 < 52주 평균 환율)': '적합' if conditions[0] else '부적합',
        '조건2 (현재 달러 지수 < 52주 평균 달러 지수)': '적합' if conditions[1] else '부적합',
        '조건3 (현재 달러 갭 비율 > 52주 평균 달러 갭 비율)': '적합' if conditions[2] else '부적합',
        '조건4 (현재 원달러 환율 < 적정 환율)': '적합' if conditions[3] else '부적합',
        '조건5 (금/은 비율 > 80)': '적합' if conditions[4] else '부적합',
        '전체조건접합성여부': final_suitability,
        '투자매수매도 조언': advice
    }

def record_to_sheet(data):
    """
    결과를 구글 스프레드시트에 기록하는 함수
    :param data: dict, 기록할 최종 결과를 포함하는 딕셔너리
    """
    try:
        # 구글 스프레드시트 API 인증 및 연결
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'), scope)
        client = gspread.authorize(creds)
        
        # 스프레드시트 열기 및 시트 선택
        sheet = client.open("적정환율알림서비스").worksheet("금_4주")
        
        # 데이터 기록
        row = [data['현재 날짜'], data['기간']] + list(data.values())[2:]
        sheet.append_row(row)
    except Exception as e:
        print(f"Error recording to Google Sheet: {e}")
        send_slack_notification(f"Error recording to Google Sheet: {e}")

def calculate_exchange_rate(period_weeks):
    """
    주요 로직을 실행하는 함수
    :param period_weeks: int, 데이터를 분석할 기간(주)
    """
    try:
        # 데이터 다운로드
        usd_index_data, usd_krw_data, gold_data, silver_data = download_data(period_weeks)
        
        # 지표 계산
        indicators = calculate_indicators(usd_index_data, usd_krw_data, gold_data, silver_data)
        
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
    except Exception as e:
        print(f"Error in calculate_exchange_rate: {e}")
        send_slack_notification(f"Error in calculate_exchange_rate: {e}")

# 4주 기준으로 데이터 계산
calculate_exchange_rate(4)
