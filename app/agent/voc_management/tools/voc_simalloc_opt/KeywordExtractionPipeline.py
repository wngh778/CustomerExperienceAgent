from agent.voc_management.utils.load_files import load_prompt
from agent.voc_management.utils.text_preprocessing import split_dataframe

from langchain_core.messages import SystemMessage
from tqdm.asyncio import tqdm_asyncio
from pathlib import Path

import pandas as pd
import asyncio
import os

class KeywordExtractionPipeline:
    """
    키워드 추출 파이프라인 클래스.

    이 클래스는 LLM을 이용해 VOC 텍스트로부터 키워드를 추출하고,
    입력 데이터프레임을 청크 단위로 비동기 처리하여 결과를 결합합니다.

    주요 기능:
    - 프롬프트 파일을 로드하여 LLM 호출에 사용할 템플릿을 준비
    - 단일 텍스트에 대한 키워드 추출 (비동기)
    - 데이터프레임을 청크로 나누어 병렬적으로 키워드 추출 수행 및 결과 병합

    프롬프트 역할:
    - 프롬프트 파일(prompt_file)은 키워드 추출 기준과 출력 형식을 정의합니다.
      내부적으로 {text} 플레이스홀더에 실제 VOC 텍스트가 주입되어 LLM이 일관된
      지침에 따라 키워드를 생성하도록 유도합니다.

    매개변수:
    - prompt_file (str): 키워드 추출용 프롬프트 파일명
    - chunk_size (int): 데이터프레임 청크 크기 (한 번에 처리할 행 수)
    - inter_chunk_delay (float): 청크 처리 사이의 지연(초). 과도한 요청을 방지하거나
      레이트 리밋을 고려할 때 사용합니다.
    """
    def __init__(
        self,
        llm,
        prompt_file: str = "keywords_extract_no_rag.txt",
        chunk_size: int = 50,
        inter_chunk_delay: float = 0.2,
    ):
        """
        파이프라인 초기화.

        - 프롬프트 파일을 로드하고, LLM 및 처리 파라미터를 설정합니다.

        매개변수:
        - prompt_file (str): 키워드 추출에 사용할 프롬프트 파일명
        - chunk_size (int): 데이터프레임을 나눌 청크 크기
        - inter_chunk_delay (float): 청크 간 대기 시간(초)
        """
        self.llm = llm
        self.prompt = load_prompt(prompt_file)
        self.chunk_size = chunk_size
        self.inter_chunk_delay = inter_chunk_delay

    async def extract_keywords(self, text: str) -> str:
        """
        단일 텍스트로부터 키워드를 추출하는 비동기 메서드.

        프롬프트 템플릿에 텍스트를 주입하여 SystemMessage로 LLM에 전달하고,
        응답에서 키워드 문자열을 반환합니다.

        매개변수:
        - text (str): VOC 텍스트

        반환값:
        - str: 추출된 키워드 문자열 (LLM 응답의 content)
        """
        formatted_prompt = self.prompt.format(text=text)
        response = await self.llm.ainvoke([SystemMessage(content=formatted_prompt)])
        return response.content.strip()

    async def process(self, init_df: pd.DataFrame) -> pd.DataFrame:
        """
        입력 데이터프레임을 청크 단위로 비동기 처리하여 키워드를 추출합니다.

        처리 흐름:
        - 데이터프레임을 chunk_size 기준으로 분할
        - 각 청크에서 'voc' 컬럼을 대상으로 키워드 추출 태스크 생성 및 병렬 실행
        - 결과를 'keywords' 컬럼으로 추가하여 청크별 데이터프레임 구성
        - 모든 청크 결과를 병합하여 최종 데이터프레임 반환
        - inter_chunk_delay가 설정된 경우 청크 사이에 대기

        요구사항:
        - init_df에 'voc' 컬럼이 존재해야 합니다.

        매개변수:
        - init_df (pd.DataFrame): VOC 텍스트를 포함한 초기 데이터프레임

        반환값:
        - pd.DataFrame: 'keywords' 컬럼이 추가된 결과 데이터프레임
        """
        chunks = split_dataframe(init_df, self.chunk_size)
        results = []

        for chunk in tqdm_asyncio(chunks, desc="Processing Chunks"):
            vocs = chunk["voc"].fillna("").tolist()
            tasks = [self.extract_keywords(v) for v in vocs]
            chunk_results = await asyncio.gather(*tasks)
            chunk = chunk.copy()
            chunk["keywords"] = chunk_results
            results.append(chunk)

            if self.inter_chunk_delay:
                await asyncio.sleep(self.inter_chunk_delay)

        return pd.concat(results, ignore_index=True)

async def run_keyword_extraction(
    init_df: pd.DataFrame,
    llm,
    chunk_size: int = 50,
    inter_chunk_delay: float = 0.2,
) -> pd.DataFrame:
    """
    키워드 추출 파이프라인 실행 함수.

    파이프라인 인스턴스를 생성하고 주어진 데이터프레임에 대해
    청크 단위 비동기 키워드 추출을 수행합니다.

    프롬프트 역할:
    - 내부적으로 'keywords_extract_no_rag.txt' 프롬프트를 로드하여
      키워드 추출 기준과 출력 포맷을 일관되게 유지합니다.

    매개변수:
    - init_df (pd.DataFrame): 'voc' 컬럼을 포함하는 입력 데이터프레임
    - llm: ainvoke 메서드를 제공하는 LLM 인스턴스
    - chunk_size (int): 청크 크기
    - inter_chunk_delay (float): 청크 간 대기 시간(초)

    반환값:
    - pd.DataFrame: 'keywords' 컬럼이 추가된 결과 데이터프레임
    """
    pipeline = KeywordExtractionPipeline(
        llm=llm,
        prompt_file="keywords_extract_no_rag.txt",
        chunk_size=chunk_size,
        inter_chunk_delay=inter_chunk_delay,
    )
    return await pipeline.process(init_df)