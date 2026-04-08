"""
스크립트 실행

```shell
$ cd {개인 compute 경로}/WP-KB0-00107-TA-0001/app/

$ python -m tests.data_analysis.cx_extractor
```
"""
import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pandas import DataFrame

# LangChain / OpenAI
from langchain.prompts import PromptTemplate
from langchain.schema import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI

from core.util import load_resource_file

VERSION = "v0"

default_prompt_path = Path(__file__).resolve().parents[0] / "sample_files"
PROMPT_FILE_NAME: str = f"cx_extract_prompt_{VERSION}.txt"

EXTRACT_PROMPT = load_resource_file(default_prompt_path / PROMPT_FILE_NAME)

# ----------------------------------------------------------------------
# 1. CSV 읽기
# ----------------------------------------------------------------------
def read_file(file_path: str) -> DataFrame:
    """
    CSV 파일을 pandas DataFrame 으로 읽어 반환합니다.
    파일이 존재하지 않으면 FileNotFoundError 를 발생시킵니다.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = pd.read_csv(file_path, dtype=str)   # 모든 컬럼을 문자열로 읽어 변환 오류 방지
    return df

# ----------------------------------------------------------------------
# 2. LLM 인스턴스 생성
# ----------------------------------------------------------------------
def get_llm() -> ChatOpenAI:
    
    llm = ChatOpenAI(  
        model="gpt-oss-120b",  
        base_url="http://stg-llmops-trnn-genaihub.kbonecloud.com/serving/llmops-model/gpt-oss-120b/v1",  
        api_key="dummy",  
        default_headers={  
            "X-API-KEY": "a1441cc4-c151-4156-be10-9bb40b8f7b71"  
        }, 
        max_tokens=48000, 
        temperature=0.2, 
        top_p = 0.9 
    )
    return llm

# ----------------------------------------------------------------------
# 3. 프롬프트 템플릿 정의
# ----------------------------------------------------------------------
def build_prompt(row: pd.Series) -> List[BaseMessage]:

    system_prompt = SystemMessage(content=EXTRACT_PROMPT)
    user_message = f"""
    # VOC 요약:
    {row.get("VOC 요약", "")}
    """
    return [system_prompt, HumanMessage(content=user_message)]
    

# ----------------------------------------------------------------------
# 4. 비동기 함수 : 한 행에 대해 CX element 추출
# ----------------------------------------------------------------------
async def generate_cx_element(row_idx: int, row: pd.Series, llm: ChatOpenAI) -> List[Dict[str, Any]]:
    """
    - `row_idx` : DataFrame 에서 행 번호 (결과를 원본 DataFrame에 바로 저장하기 위함)
    - `row`     : pandas Series (한 행)
    - `llm`     : LangChain LLM 인스턴스

    반환값은 None이며, 함수 내부에서 `row` 에 새로운 컬럼을 직접 삽입합니다.
    """
    """
    LLM 호출 → JSON 파싱 → result 리스트 반환
    에러가 발생하면 빈 리스트([]) 반환
    """
    brief = row.get("VOC 요약")
    
    # 1️⃣ 프롬프트 만들기
    prompt = build_prompt(row)

    # 2️⃣ LLM 호출 (async)
    try:
        raw_response: str = await llm.ainvoke(prompt)   # type: ignore[arg-type]
    except Exception as exc:
        print(f"[ERROR] LLM 호출 실패 (row {row_idx}): {exc}")
        return []          # 빈 리스트 반환

    # 3️⃣ JSON 파싱
    json_parser = JsonOutputParser()
    try:
        parsed: Dict[str, Any] = json_parser.parse(raw_response.content)  # type: ignore[assignment]
    except Exception as exc:
        print(f"[ERROR] JSON 파싱 실패 (row {row_idx}): {exc}\nRaw response: {raw_response}")
        return []

    # 4️⃣ 결과 추출
    try:
        result = parsed.get("result", [])
        # LLM이 단일 dict 를 반환했을 경우에도 리스트 형태로 맞춰줍니다.
        if isinstance(result, dict):
            result = [result]
        elif not isinstance(result, list):
            result = []
        return result
    except Exception as exc:
        print(f"[ERROR] 결과 추출 실패 (row {row_idx}): {exc}\nParsed: {parsed}")
        return []
    
BATCH_SIZE = 150
async def extract_cx_element_by_csv(base_year: str, target_channel_code: str) -> None:
    """
    CSV 를 읽고, LLM 로 CX element 를 추출한 뒤, 결과를 새로운 CSV 로 저장합니다.
    """

    # 1️⃣ CSV 읽기
    input_path: str = f"{base_year}_td_nps_{target_channel_code}.csv"
    output_path: str = f"{base_year}_td_nps_{target_channel_code}_with_cx.csv"
    
    default_input_path = Path(__file__).resolve().parents[0] / "sample_files"
    default_output_path = Path(__file__).resolve().parents[0] / "output"
    
    df: pd.DataFrame = read_file(str(default_input_path / input_path))

    # 2️⃣ result 컬럼을 미리 생성 (빈 리스트)
    df["result"] = [[] for _ in range(len(df))]

    # 3️⃣ LLM 준비
    llm = get_llm()

    total_rows = len(df)
    print(f"[INFO] 전체 행 수: {total_rows}")

    # 4️⃣ 배치 단위 비동기 처리
    for start in range(0, total_rows, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_rows)
        batch_idx = list(range(start, end))

        # 각 행에 대해 generate_cx_element 를 호출하고, 반환값을 바로 df에 저장
        tasks = [
            generate_cx_element(idx, df.iloc[idx], llm)   # 반환값은 List[dict]
            for idx in batch_idx
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for idx, res in zip(batch_idx, results):
            if isinstance(res, Exception):
                # generate_cx_element 내부에서도 예외를 잡아 빈 리스트를 반환했지만,
                # 혹시 모를 예외가 여기서 튀면 로깅하고 빈 리스트 저장
                print(f"[ERROR] 배치 작업 중 예외 (row {idx}): {res}")
                df.at[idx, "result"] = []
            else:
                # 정상 반환값 (List[dict]) 을 바로 저장
                df.at[idx, "result"] = res

        print(f"[INFO] 배치 {start // BATCH_SIZE + 1} / {(total_rows - 1) // BATCH_SIZE + 1} 완료")

    # --------------------------------------------------------------
    # 5️⃣ result 를 explode → dict → 컬럼 변환
    # --------------------------------------------------------------
    # 5‑1) explode (list → 여러 행)
    df_exploded = df.explode("result").reset_index(drop=True)

    # 5‑2) dict → 컬럼 (json_normalize)
    dicts = df_exploded["result"].apply(lambda x: x if isinstance(x, dict) else {})
    dict_df = pd.json_normalize(dicts)

    # 5‑3) 원본 컬럼과 병합
    df_exploded = df_exploded.drop(columns=["result"])
    merged = pd.concat([df_exploded, dict_df], axis=1)

    # 5‑4) 같은 원본 행이 여러 번 나타날 때, 첫 번째 행만 원본값 유지
    original_cols = [c for c in df.columns if c != "result"]
    merged["_tmp_row_id"] = merged.groupby(original_cols).cumcount()
    mask_not_first = merged["_tmp_row_id"] > 0
    merged.loc[mask_not_first, original_cols] = ""
    merged = merged.drop(columns=["_tmp_row_id"])

    # 5‑5) NaN → 빈 문자열
    merged = merged.fillna("")

    # 6️⃣ 출력 디렉터리 생성 & CSV 저장
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    merged.to_csv(default_output_path / output_path, index=False, encoding="utf-8-sig")
    print(f"[SUCCESS] 결과 CSV 저장 완료 → {default_output_path / output_path}")




# ----------------------------------------------------------------------
# 6. 스크립트 엔트리 포인트
# ----------------------------------------------------------------------
if __name__ == "__main__":

    # ==== 타겟 데이터 설정 =====
    base_year = '2025'
    target_channel_code = "03"
    version = 'v0'
    # ===========================
    
    # 기본적으로 asyncio.run 으로 비동기 함수를 실행합니다.
    asyncio.run(extract_cx_element_by_csv(base_year, target_channel_code))
    

