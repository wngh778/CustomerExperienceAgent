import os
import asyncio
from pathlib import Path
import re
import itertools
import time

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Coroutine

from langchain_openai import ChatOpenAI
from langchain.schema import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser

from core.config import settings
from core.util import load_resource_file, create_azurechatopenai
from core.logger import get_logger

from agent.agent_template import Agent
from agent.data_analysis.utils import DataAnalysisUtils

from agent.data_analysis.model.dto import VocFilterRequestInfo
from agent.data_analysis.model.vo import VocInfo
from agent.data_analysis.model.consts import VocFilterMessage, CommonCode


# resources 기본 경로 설정
default_resource_path = str(Path(__file__).resolve().parents[2] / 'resources')

# 상수 - 프롬프트
SYSTEM_PROMPT_NO_STGE_FILE_NAME = f"voc_filter_no_stage_system.txt"
SYSTEM_PROMPT_STGE_FILE_NAME = f"voc_filter_stage_system.txt"
HUMAN_PROMPT_FILE_NAME = f"voc_filter_human.txt"

# 상수 - 딕셔너리 키
STAGE_EXIST="99"
STAGE_NOT_EXIST="00"


logger = get_logger("voc_filter")

class VocFilter(Agent):
    """
    Voc Filter (VOC 필터):
    일처리 대상 voc 중 분석대상  voc 식별(일정 음절 미만 voc 삭제 등)	
    ================================================================
    기능:
    1. 입력된 VOC 분석 대상 중복 제거
    
    2. 입력된 VOC 분석 대상 필터링

    3. NLP - 문항설문조사채널 / 고객경험단계구분과 관련있는 VOC인지 필터링
    """
    
    def __init__(self, llm, req_dto: VocFilterRequestInfo,
                 ch_stge_dict: Dict[str, List[str]],
                 prompt_path: Optional[str] = default_resource_path + '/prompt') -> None:
        """
        초기화
        """
        self.req_dto = req_dto
        self.ch_stge_dict = ch_stge_dict
        
        # 1. Patterns 초기화
        self.init_patterns()

        # 2. llm Chat 모델 인스턴스 생성
        self.llm = llm
        self.failback_llm = llm
        self.json_parser = JsonOutputParser()

        # 3. 프롬프트 초기화
        self.load_resources(prompt_path)

        self.chunk_cnt: int = 0


    def load_resources(self, prompt_path: str) -> None:
        """
        system prompt loading
        """

        # VOC Filtering 시스템 프롬프트 - No Stage
        no_stge_system_prompt_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_NO_STGE_FILE_NAME)
        self.no_stge_system_prompt_str: str = no_stge_system_prompt_str.format(
            RESPONSE_KEY=CommonCode.RESPONSE_KEY.value,
            RESULT_KEY=CommonCode.RESULT_KEY.value,
            cx_stge_list="{cx_stge_list}" # 이 값은 실행 시점에 넣을거임
        )

        # VOC Filtering 시스템 프롬프트 - Stage
        stge_system_prompt_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_STGE_FILE_NAME)
        stge_system_prompt_formatted = stge_system_prompt_str.format(
            RESPONSE_KEY=CommonCode.RESPONSE_KEY.value,
            RESULT_KEY=CommonCode.RESULT_KEY.value,
        )
        self.stge_system_prompt: SystemMessage = SystemMessage(content=stge_system_prompt_formatted)

        
        # 공통 휴면 프롬프트
        self.human_prompt_str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_FILE_NAME)

        return

    def load_langfuse_resources(self) -> None:
        return


    def init_patterns(self) -> None:
        # 한글 자음·모음 전용 문자(자모) 패턴
        self.jamo_pattern = re.compile(r'[\u1100-\u11FF\u3130-\u318F\uA960-\uA97F\uD7B0-\uD7FF]')
        # 숫자-특수문자 제거 (한글 완성형(가~힣) 과 영문 알파벳(a‑z, A‑Z) 그리고 공백만 남긴다.)
        self.number_special_character_pattern = re.compile(r'[^가-힣a-zA-Z\s]')
        
        
    
    async def execute(self, voc_infos: List[VocInfo]) -> List[VocInfo]:
        """
        실제 실행 부
        1. VOC 중복 제거
        2. VOC 필터링(특수문자 & 자모 & 반복 & 10음절)
        3. NLP - VOC 필터링(채널 / 고객경험단계)
        """

        # 1. VOC 중복 제거 (Pk 기준)
        self.distinct_voc_list(voc_infos)
        
        # 2. VOC 필터링
        for voc_info in voc_infos:

            # 중복 제거된 VOC인지 체크
            if voc_info.filtered_yn:
                continue

            self.filter_voc_content(voc_info)

        # 3. NLP VOC 필터링
        # 3-1. 채널 / 고객경험단계 별 VocInfo list Dict 세팅
        voc_group_dict = self.convert(voc_infos)
        
        # 3-2. NLP Tasks 배열 생성
        tasks = []
        for ch_cd, ch_voc_dict in voc_group_dict.items():
            tasks.extend(self.create_nlp_tasks(ch_cd, ch_voc_dict))
        
        # 3-3. 배치 사이즈 단위로 실제 실행
        batch_size = self.req_dto.batch_size
        for i in range(0, len(tasks), batch_size):
            task_chunk = tasks[i:i + batch_size]
            # gather() 에는 * 연산자를 통해 인자 풀기
            await asyncio.gather(*task_chunk)
            await asyncio.sleep(self.req_dto.batch_sleep)
            
        return voc_infos


    def create_nlp_tasks(self, ch_cd: str, ch_voc_dict: Dict[str, List[VocInfo]]) -> List[Coroutine[Any, Any, None]]:
        """
        채널 별 VOC 필터링 수행 (NLP)의 코루틴 리스트 생성
        """
        tasks = []
        chunk_size = self.req_dto.voc_chunk_size
        
        # 1. chunk 분리 (고객경험단계의 유무 / chunk 사이즈)
        for exist_stage_str, voc_list in ch_voc_dict.items():
            # 2. 고객경험요소 유무 체크
            exist_stage = False
            if exist_stage_str == STAGE_EXIST:
                exist_stage = True

            # 3. Chunk 사이즈로 분리
            for i in range(0, len(voc_list), chunk_size):
                voc_chunk = voc_list[i:i + chunk_size]
                tasks.append(self.generate_voc_filter_wrap(self.chunk_cnt, ch_cd, voc_chunk, exist_stage))
                self.chunk_cnt = self.chunk_cnt + 1

        return tasks
            
        
    async def generate_voc_filter_wrap(self, chunk_index: int, ch_cd: str, voc_list: List[VocInfo], exist_stage: bool) -> None:
        """
        각 청크 단위로 VOC 필터링 wrapping
        ==================================
        - logging
        - try-catch
        """
        # 1. VOC 필터링 시작 시간
        generate_start_time = time.time()

        try:
            # 2-1. VOC 필터링 실행 부분
            await self.generate_voc_filter(chunk_index, ch_cd, voc_list, exist_stage)
                        
        except Exception as e:
            # TODO: 예외 처리 추가 -> SKIP (voc 제외? / 미분류 기본값?)
            logger.exception("에러 발생 -> SKIP")
            
        finally:
            # 3. VOC 필터링 종료 시간
            generate_end_time = time.time()
                
            elapsed_time = generate_start_time - generate_end_time


    async def generate_voc_filter(self, chunk_index: int, ch_cd: str, voc_list: List[VocInfo], exist_stage: bool) -> None:
        """
        각 청크 단위로 VOC 필터링 수행
        """
        
        # 1. LLM 요청
        generated_response = await DataAnalysisUtils.ainvoke_llm(self.get_prompt(ch_cd, voc_list, exist_stage), self.llm, self.failback_llm)

        # 2. result setting
        self.valid_and_set_result(voc_list, generated_response.content)

        

    def valid_and_set_result(self, voc_list: List[VocInfo], content: str) -> None:
        """
        생성된 결과 검증 및 결과 세팅
        """
        # 1. 빈 문자열일 경우, 별도의 처리 없음
        if not content or not content.strip():
            return

        # 2. 생성 응답 Parsing
        parsed_response: Dict[str, List[str]] = DataAnalysisUtils.parse_llm_xml_to_dict(content)
        
        filter_target_ids = parsed_response.get(CommonCode.RESPONSE_KEY.value, {}).get(CommonCode.RESULT_KEY.value)
        if not filter_target_ids:
            return
        elif isinstance(filter_target_ids, str):
            filter_target_ids = [ filter_target_ids ]

        # 3. 필터링 대상 ID 리스트 처리
        for target_id in filter_target_ids:
            index = int(target_id)
            voc_info = voc_list[index]
            # 필터링
            voc_info.filtered_yn = True
            voc_info.filtered_reason = VocFilterMessage.CH_STGE_NOT_RELATED.value

        return

    

    def get_prompt(self, ch_cd: str, voc_infos: List[VocInfo], exist_stage: bool) -> List[BaseMessage]:
        """
        고객경험단계구분의 유무('00' - 해당무)에 따라
        프롬프트 라우팅
        """
        cx_stage_list: List[str] = self.ch_stge_dict[ch_cd]
        
        if exist_stage:
            return self._get_prompt_stge(cx_stage_list, voc_infos)
        else:
            return self._get_prompt_no_stge(cx_stage_list, voc_infos)
            


    def _get_prompt_stge(self, cx_stage_list: List[str], voc_infos: List[VocInfo]) -> List[BaseMessage]:
        """
        고객경험요소구분이 존재할 경우, 특정 단계로 연관성 판단
        """

        index = 0
        
        voc_input_rows = []
        for voc in voc_infos:
            voc_row = f"""
            <voc_itme id={index}>
                <VOC내용>{voc.orin_voc_content}</VOC내용>
                <설문조사대상채널>{voc.final_qsitm_pol_taget_nm}</설문조사대상채널>
                <고객경험단계구분>{voc.cx_stge_dstic_nm}</고객경험단계구분>
            </voc_item>
            """
            voc_input_rows.append(voc_row)
            index = index + 1
        
        voc_context = "\n".join(voc_input_rows)
        
        human_prompt_str = self.human_prompt_str.format(voc_list=voc_context)
        
        return [ self.stge_system_prompt, HumanMessage(content=human_prompt_str) ]


    
    def _get_prompt_no_stge(self, cx_stage_list: List[str], voc_infos: List[VocInfo]) -> List[BaseMessage]:
        """
        고객경험요소구분이 '00'일 경우, 모든 단계로 연관성 판단
        """

        index = 0
        
        voc_input_rows = []
        for voc in voc_infos:
            voc_row = f"""
            <voc_itme id={index}>
                <VOC내용>{voc.orin_voc_content}</VOC내용>
                <설문조사대상채널>{voc.final_qsitm_pol_taget_nm}</설문조사대상채널>
            </voc_item>
            """
            voc_input_rows.append(voc_row)
            index = index + 1
        
        voc_context = "\n".join(voc_input_rows)

        # 프롬프트 세팅
        no_stge_system_formatted = self.no_stge_system_prompt_str.format(
            cx_stge_list=cx_stage_list
        )
        human_prompt_str = self.human_prompt_str.format(voc_list=voc_context)
        
        return [ SystemMessage(content=no_stge_system_formatted), HumanMessage(content=human_prompt_str) ]

        
        
    
    def distinct_voc_list(self, voc_infos: List[VocInfo]) -> None:
        """
        voc_infos 안에 있는 VocInfo 객체들을 PK 값으로 중복 검사한다.
        - 첫 번째로 등장한 객체는 그대로 유지한다.
        - 이후에 같은 PK를 가진 객체는 `filtered_yn = True` 로 표시한다.
        """
        
        # PK 튜플 → 최초 등장 객체(인덱스) 매핑
        seen: Dict[Tuple, int] = {}
    
        for idx, voc_info in enumerate(voc_infos):
            # PK 값을 튜플 형태로 만든다 → 해시 가능하도록
            voc_pk_dict = voc_info.get_pk_dict()
            
            pk_tuple = tuple(voc_pk_dict.values())
    
            if pk_tuple not in seen:
                # 처음 보는 PK → 기록만 해 둔다
                seen[pk_tuple] = idx
            else:
                # 이미 존재하는 PK → 중복 처리
                voc_info.filtered_yn = True
                voc_info.filtered_reason = VocFilterMessage.DUPLICATE_PK.value

        
        return


        
    def filter_voc_content(self, voc_info: VocInfo) -> None:
        """
        VOC 필터링
        # ------------------------------------------------------------
        # 1) 한글 자음·모음 전용 문자(자모) 제거
        #    - 완성형 한글(가~힣) 은 그대로 두고,
        #    - Hangul Jamo(초·중·종성 자모) 영역을 모두 제외한다.
        #    Unicode 범위
        #      * U+1100‑U+11FF   (Hangul Jamo)
        #      * U+3130‑U+318F   (Hangul Compatibility Jamo)
        #      * U+A960‑U+A97F   (Hangul Jamo Extended‑A)
        #      * U+D7B0‑U+D7FF   (Hangul Jamo Extended‑B)
        # ------------------------------------------------------------
        
        # ------------------------------------------------------------
        # 2) 숫자·특수문자 제거
        #    - 한글 완성형(가~힣) 과 영문 알파벳(a‑z, A‑Z) 그리고 공백만 남긴다.
        # ------------------------------------------------------------

        # ------------------------------------------------------------
        # 3) 연속된 반복 문자(distinct) → "aaabbc" → "abc"
        # ------------------------------------------------------------

        # ------------------------------------------------------------
        # 4) 글자수 체크
        #    - 결과 문자열이 **한 글자**이거나 **공백**이면 있으면 False,
        #      그 외(두 글자 이상)면 True를 반환한다.
        # ------------------------------------------------------------
        """
        # 0. 공백 제거
        cleaned_voc_content: str = voc_info.orin_voc_content.replace(" ", "")
        
        # 1. 자모 제거
        cleaned_voc_content = self.jamo_pattern.sub('', cleaned_voc_content)

        # 2. 숫자·특수문자 제거
        cleaned_voc_content = self.number_special_character_pattern.sub('', cleaned_voc_content)

        # 3. 연속된 반복 문자 collapse
        cleaned_voc_content = self._collapse_repeats(cleaned_voc_content)

        # 4. 길이 체크
        ok = self._length_check(cleaned_voc_content, self.req_dto.voc_length_limit)

        if not ok:
            voc_info.filtered_yn = True
            voc_info.filtered_reason = VocFilterMessage.TEXT_FILTER.value

        return
        
        
    @staticmethod
    def _collapse_repeats(text: str) -> str:
        """연속된 같은 문자를 하나만 남긴다."""
        return ''.join(k for k, _ in itertools.groupby(text))

    @staticmethod
    def _length_check(text: str, lenth_limit: int) -> bool:
        """공백이거나 열 글자이면 False, 그 외는 True."""
        return len(text) > lenth_limit

    @staticmethod
    def convert(voc_list: List[VocInfo]) -> Dict[str, Dict[str, List[VocInfo]]]:
        """
        voc_list 에서 filtered_yn == False 인 항목만 남기고,
        final_qsitm_pol_taget_dstcd → 고객경험단계 있음('99') / 없음('00') → List[VocInfo] 로
        중첩 딕셔너리를 만든다.
        """
        # 1) filtered_yn == False 인 것만 추출
        filtered = [v for v in voc_list if not v.filtered_yn]
    
        # 2) 2단계 중첩 defaultdict 생성
        #    outer: final_qsitm_pol_taget_dstcd → inner dict
        #    inner: qsitm_pol_taget_dstcd → list of VocInfo
        result: Dict[str, Dict[str, List[VocInfo]]] = defaultdict(
            lambda: defaultdict(list)   # inner dict 자동 생성
        )
    
        # 3) 한 번에 그룹핑
        for v in filtered:
            outer_key = v.final_qsitm_pol_taget_dstcd
            inner_key = STAGE_EXIST
            if v.cx_stge_dstcd == '00': # 해당무
                inner_key = STAGE_NOT_EXIST
            result[outer_key][inner_key].append(v)
    
        # defaultdict 를 일반 dict 로 변환 (선택 사항)
        # 이렇게 하면 출력 시 `defaultdict` 표기가 사라진다.
        result = {
            outer_k: dict(inner_dict)   # inner defaultdict → dict
            for outer_k, inner_dict in result.items()
        }
    
        return result
    