from agent.voc_management.utils.load_files import load_query
from pathlib import Path
import pandas as pd
import asyncio
import re

class KeywordCxMatcher:
    """
    키워드와 고객 경험(CX) 요소를 기반으로 MySQL에서 관련 응답 데이터를 조회하고,
    입력 데이터프레임의 각 행에 대해 매칭 결과를 생성하는 비동기 매처 클래스.

    프롬프트/역할:
    - 입력: keywords_extract 데이터프레임(각 행에 keywords, cxc 등의 필드 포함)
    - 처리: 키워드 정규화 → 정규식 생성 → SQL 템플릿 로드 및 실행 → 결과 레코드와 입력 행 병합
    - 출력: 매칭된 응답 및 메타데이터(문항응답내용, 과제검토구분 등)를 포함하는 데이터프레임

    의존성:
    - mcp_executor: execute_tool("mysql_query", {"query": sql}) 형태로 호출 가능한 실행기
    - load_query: SQL 템플릿 파일을 로드하는 함수
    """

    def __init__(self, mcp_executor):
        """
        매처를 초기화하고 외부 실행기를 주입한다.

        매개변수:
        - mcp_executor: MySQL 쿼리 실행을 위한 외부 실행기 객체
        """
        self.executor = mcp_executor

    @staticmethod
    def _normalize_keywords(raw):
        """
        키워드 입력을 정규화하여 리스트 형태로 반환한다.

        동작:
        - 리스트/튜플: 각 요소를 문자열로 변환 후 양끝 공백 제거, 빈 문자열 제거
        - 문자열: 콤마 또는 공백 단위로 분리, 각 토큰 공백 제거, 빈 문자열 제거
        - 기타: 빈 리스트 반환

        매개변수:
        - raw: 키워드 원본 입력(리스트/튜플/문자열/기타)

        반환값:
        - 정규화된 키워드 리스트(List[str])
        """
        if isinstance(raw, (list, tuple)):
            return [s for s in (str(k).strip() for k in raw) if s]
        if isinstance(raw, str):
            return [s for s in (p.strip() for p in re.split(r"[,\s]+", raw)) if s]
        return []

    @staticmethod
    def _make_default_row(row_dict):
        """
        매칭 실패 또는 예외 발생 시 기본 결과 행을 생성한다.

        동작:
        - 입력 행(row_dict)을 복사하고 결과 관련 필드를 None으로 채운 단일 행 리스트 반환

        매개변수:
        - row_dict: 입력 행 딕셔너리

        반환값:
        - 기본 결과 행을 담은 리스트(List[Dict])
        """
        new_row = dict(row_dict)
        new_row["문항응답내용"] = None
        new_row["과제검토구분"] = None
        new_row["과제검토의견내용"] = None
        new_row["작성년월일시"] = None
        new_row["매칭키워드"] = None
        new_row["과제추진사업내용"] = None
        new_row["개선이행시작년월일"] = None
        new_row["개선이행종료년월일"] = None
        return [new_row]

    async def _process_row(self, row_dict):
        """
        단일 입력 행에 대해 키워드와 CX 값을 사용하여 DB 조회 및 매칭 결과를 생성한다.

        처리 단계:
        1) 키워드 정규화 및 정규식 패턴 구성
        2) CX 값 정규식 구성(빈 값이면 전체 매칭)
        3) SQL 템플릿 로드 및 포맷팅
        4) 실행기 통해 MySQL 쿼리 수행
        5) 응답 내용에서 실제 매칭된 키워드 탐지(소문자 비교)
        6) 입력 행과 DB 결과를 병합하여 결과 행 리스트로 반환
        7) 예외 또는 미매칭 시 기본 결과 행 반환

        매개변수:
        - row_dict: 입력 행 딕셔너리

        반환값:
        - 결과 행 리스트(List[Dict])
        """
        try:
            executor = self.executor
            norm = self._normalize_keywords(row_dict.get("keywords"))
            cx_val = row_dict.get("cxc")

            pattern = "|".join(re.escape(k) for k in norm) or ".*"
            cx_regex = re.escape(str(cx_val)) if cx_val not in (None, "") else ".*"

            base_query = load_query("match_keywords_cxElmntCtnt.sql")
            query = base_query.format(pattern=pattern, cx_regex=cx_regex)

            res = await executor.execute_tool("mysql_query", {"query": query}) or []
            if not res:
                return self._make_default_row(row_dict)

            rows_out = []
            for r in res:
                resp = r.get("문항응답내용")
                lower_resp = resp.lower() if isinstance(resp, str) else ""
                matched = next((k for k in norm if k and k.strip().lower() in lower_resp), None)

                new_row = dict(row_dict)
                new_row.update({
                    "문항응답내용": r.get("문항응답내용"),
                    "과제검토구분": r.get("과제검토구분"),
                    "과제검토의견내용": r.get("과제검토의견내용"),
                    "작성년월일시": r.get("작성년월일시"),
                    "매칭키워드": matched,
                    "과제추진사업내용": r.get("과제추진사업내용"),
                    "개선이행시작년월일": r.get("개선이행시작년월일"),
                    "개선이행종료년월일": r.get("개선이행종료년월일"),
                })
                rows_out.append(new_row)

            return rows_out

        except Exception:
            return self._make_default_row(row_dict)

    async def match_keywords_cxElmntCtnt(self, keywords_extract: pd.DataFrame) -> pd.DataFrame:
        """
        입력 데이터프레임의 각 행에 대해 _process_row를 비동기로 수행하고,
        모든 결과를 결합하여 단일 데이터프레임으로 반환한다.

        동작:
        - 각 행을 asyncio.create_task로 처리
        - asyncio.gather로 병렬 수집
        - 결과 행들을 합쳐서 데이터프레임으로 변환 후 인덱스 리셋

        매개변수:
        - keywords_extract: 키워드 및 CX 정보를 포함한 입력 데이터프레임

        반환값:
        - 매칭 결과를 담은 데이터프레임(pd.DataFrame)
        """
        result_rows_all = []
        tasks = [asyncio.create_task(self._process_row(dict(row))) for _, row in keywords_extract.iterrows()]
        results_per_task = await asyncio.gather(*tasks)
        for rows_out in results_per_task:
            result_rows_all.extend(rows_out)
        return pd.DataFrame(result_rows_all).reset_index(drop=True)