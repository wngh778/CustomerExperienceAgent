from agent.voc_management.utils.load_files import load_prompt
from langchain_core.messages import SystemMessage, HumanMessage
from tqdm.asyncio import tqdm_asyncio
from typing import List, Optional
from pydantic import BaseModel, Field

import pandas as pd
import asyncio
import ast

class VOCSelectionResult(BaseModel):
    """Output schema for VOC relevance selection"""
    selected_vocs: List[int] = Field(description="List of selected VOCs' id")

class RelevanceMatcher:
    """
    LLM을 사용해 VOC와 제안 목록의 관련 항목을 찾아내고, 그룹별로 결과를 정제하는 매처 클래스.
    """

    def __init__(self, llm, prompt_name="relevance_check.txt", chunk_size=5, chunk_delay=1.0):
        """
        매처 초기화 메서드.
        - llm: 비동기 호출 가능한 LLM 객체
        - prompt_name: 프롬프트 파일명
        - chunk_size: 그룹 처리를 위한 병렬 처리 묶음 크기
        - chunk_delay: 각 묶음 처리 사이 지연 시간(초)
        """
        self.llm = llm
        self.prompt_name = prompt_name
        self.chunk_size = chunk_size
        self.chunk_delay = chunk_delay


    async def get_matching_indices(self, voc: str, suggestions: list, created_at: str = "") -> List[int]:
        """
        VOC와 제안 목록을 LLM에 전달해 관련 있는 항목의 인덱스 목록을 반환.
        - voc: VOC 텍스트
        - suggestions: 제안(문항응답내용) 리스트
        - created_at: 작성일시 문자열
        반환: 관련 항목 인덱스 리스트(정수). 실패 시 빈 리스트.
        """
        prompt_template = load_prompt(self.prompt_name)

        similar_voc_str = ""
        for i, voc in enumerate(similar_voc):
            if voc is not None:
                similar_voc_str += f"<voc id={i}> {voc} </voc>\n"
            
        voc_str = "" if voc is None else str(voc)
        created_at_str = "" if created_at is None else str(created_at)
        
        prompt = prompt_template.format(
            created_at=created_at_str
        )

        try:
            resp = await self.llm.ainvoke([SystemMessage(content=prompt)])
            text = getattr(resp, "content", "")
            text = text.strip()
            indices = ast.literal_eval(text)

            if not isinstance(indices, list):
                return []
            clean = [int(i) for i in indices if isinstance(i, int)]
            return clean
        except Exception:
            return []

    async def process_group(self, custIdnfr, group):
        """
        동일 qusnInvlTagtpUniqID 그룹에서 LLM으로 관련 응답을 추출해 해당 행들만 반환.
        관련 항목이 없거나 오류 시 첫 행만 남기고 특정 컬럼을 None으로 설정해 반환.
        - custIdnfr: 고객 식별자(그룹 키)
        - group: 해당 그룹의 DataFrame
        반환: 관련 행들로 구성된 DataFrame 또는 폴백 DataFrame.
        """
        voc = group.iloc[0].get("voc", None)
        suggestions = group["문항응답내용"].tolist()
        created_at = group.iloc[0].get("작성년월일시", "")

        indices = await self.get_matching_indices(voc, suggestions, created_at)

        fallback_cols = [
            "문항응답내용",
            "과제검토구분",
            "과제검토의견내용",
            "작성년월일시",
            "매칭키워드",
            "과제추진사업내용",
            "개선이행시작년월일",
            "개선이행종료년월일",
        ]

        if not indices:
            first_row_df = group.iloc[[0]].copy()
            for col in fallback_cols:
                if col in first_row_df.columns:
                    first_row_df[col] = None
            return first_row_df

        matched = []
        for idx in indices:
            if 0 <= idx < len(group):
                matched.append(group.iloc[idx])

        if not matched:
            first_row_df = group.iloc[[0]].copy()
            for col in fallback_cols:
                if col in first_row_df.columns:
                    first_row_df[col] = None
            return first_row_df

        out_df = pd.DataFrame(matched)
        return out_df

    async def refine_match_df(self, match_df: pd.DataFrame, chunk_size: int = None, chunk_delay: float = None) -> pd.DataFrame:
        """
        입력 DataFrame을 qusnInvlTagtpUniqID로 그룹화하여 비동기로 처리하고,
        LLM 기반 관련 항목만 모아 정제된 DataFrame을 반환.
        - match_df: 원본 DataFrame
        - chunk_size: 병렬 처리 묶음 크기(미지정 시 초기화 값 사용)
        - chunk_delay: 묶음 처리 사이 지연 시간(초, 미지정 시 초기화 값 사용)
        반환: 관련 행들만 포함한 정제된 DataFrame. 폴백 로직 포함.
        """
        if chunk_size is None:
            chunk_size = self.chunk_size
        if chunk_delay is None:
            chunk_delay = self.chunk_delay

        match_df = match_df.drop_duplicates()

        fallback_cols = [
            "문항응답내용",
            "과제검토구분",
            "과제검토의견내용",
            "작성년월일시",
            "매칭키워드",
            "과제추진사업내용",
            "개선이행시작년월일",
            "개선이행종료년월일",
        ]

        if "qusnInvlTagtpUniqID" not in match_df.columns:
            if len(match_df) > 0:
                first_row_df = match_df.iloc[[0]].copy()
                for col in fallback_cols:
                    if col in first_row_df.columns:
                        first_row_df[col] = None
                return first_row_df
            else:
                return pd.DataFrame(columns=match_df.columns)

        groups = list(match_df.groupby("qusnInvlTagtpUniqID"))
        total = len(groups)

        results = []
        for i in tqdm_asyncio(range(0, total, chunk_size), desc="Processing groups"):
            chunk = groups[i:i + chunk_size]
            tasks = [self.process_group(idx, grp) for idx, grp in chunk]
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
            results.extend([r for r in chunk_results if not isinstance(r, Exception)])

            if chunk_delay and (i + chunk_size) < total:
                await asyncio.sleep(chunk_delay)

        non_empty = [df for df in results if isinstance(df, pd.DataFrame) and not df.empty]

        if not non_empty:
            if len(match_df) > 0:
                first_row_df = match_df.iloc[[0]].copy()
                for col in fallback_cols:
                    if col in first_row_df.columns:
                        first_row_df[col] = None
                return first_row_df
            else:
                return pd.DataFrame(columns=match_df.columns)

        refined = pd.concat(non_empty, axis=0).sort_index()
        refined = refined.reset_index(drop=True)
        return refined

# 예시
# matcher = RelevanceMatcher()
# refined_df = await matcher.refine_match_df(match_df)