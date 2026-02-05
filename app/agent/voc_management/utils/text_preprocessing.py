import re 
import pandas as pd
from typing import Dict, List, Any, Tuple

# 텍스트 전처리 함수
def clean_text(s):
    return re.sub(r'[^0-9A-Za-z가-힣\s]', '', s)

def preprocess_text(text):
    if isinstance(text, str):  # 문자열인지 확인
        text = text.strip()  # 앞뒤 공백 제거
        text = text.lower()  # 소문자로 변환
        text = text.replace(".", "")  # 마침표 제거
        text = text.replace(",", "")  # 쉼표 제거 (필요 시 추가)
        # 추가적으로 필요한 전처리 작업이 있다면 여기에 추가
    return text

# 파일 읽기 함수
def read_file(file_path):
    try:
        if file_path.endswith('.txt'):  # 텍스트 파일 처리
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            return content
        elif file_path.endswith('.csv'):  # CSV 파일 처리
            df = pd.read_csv(file_path)
            return df
        elif file_path.endswith('.xlsx') or file_path.endswith('.xls'):  # 엑셀 파일 처리
            df = pd.read_excel(file_path)
            return df
        else:
            raise ValueError("지원하지 않는 파일 형식입니다. .txt, .csv 또는 .xlsx/.xls 파일만 지원됩니다.")
    except Exception as e:
        print(f"파일을 읽는 중 오류가 발생했습니다: {e}")
        return None

# 파일 저장 함수
def save_file(content, file_path):

    try:
        if isinstance(content, str):  # 문자열 저장
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(content)
        elif isinstance(content, pd.DataFrame):  # 데이터프레임 저장
            content.to_csv(file_path, index=False, encoding='utf-8')
        else:
            raise ValueError("저장할 내용의 형식이 잘못되었습니다. 문자열 또는 pandas DataFrame만 지원됩니다.")
    except Exception as e:
        print(f"파일을 저장하는 중 오류가 발생했습니다: {e}")

def split_dataframe(df: pd.DataFrame, chunk_size: int) -> List[pd.DataFrame]:
    """DataFrame을 chunk_size 만큼 나눠 리스트 반환."""
    return [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]