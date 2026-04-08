import os
import asyncio
from pathlib import Path
import time

from typing import Any, Dict, List, Optional, Set
from collections import defaultdict


from langchain.schema import BaseMessage, HumanMessage, SystemMessage
from langchain_core.output_parsers import JsonOutputParser

from core.config import settings
from core.util import load_resource_file, create_azurechatopenai
from core.logger import get_logger

from agent.agent_template import Agent
from agent.data_analysis.utils import DataAnalysisUtils

from agent.data_analysis.model.dto import EntityWordDetectRequestInfo
from agent.data_analysis.model.vo import VocInfo
from agent.data_analysis.model.consts import CommonCode, EntityWordDetectMessage


# resources 기본 경로 설정
default_resource_path = str(Path(__file__).resolve().parents[2] / 'resources')

# 상수 - 프롬프트
VERSION = "v0"
# 상수 - 개체어 용어 키값
PRODUCT_SERVICE_KEY="product_service"
PERFORMANCE_QUALITY_KEY="performance_quality"


# 프롬프트 세팅
SYSTEM_PROMPT_CXE_FILE_NAME = f"entity_word_detect_cxe_system.txt"
HUMAN_PROMPT_CXE_FILE_NAME = f"entity_word_detect_cxe_human.txt"
SYSTEM_PROMPT_NO_CXE_FILE_NAME = f"entity_word_detect_no_cxe_system.txt"
HUMAN_PROMPT_NO_CXE_FILE_NAME = f"entity_word_detect_no_cxe_human.txt"


logger = get_logger("entity_word_detector")


class EntityWordDetector(Agent):
    """
    EntityWordDetector (개체어 추출):
    고객경험요소 추출 유무에 따라 Context를 달리하며 개체어(상품서비스용어, 성능품질용어)를 추출한다.
    ================================================================
    기능:
    1. Context 세팅 (고객경험요소 추출/미추출) 
        - 추출일 경우, VOC원문 내용 중에서도 대표 고객경험요소에 가장 적합한 내용을 포커스로 개체어를 추출 
        - 미추출일 경우, VOC원문 내용 중에서도 채널, 고객경험단계를 포커스로 개체어를 추출
    2. LLM 실행
    3. 최종 결과 세팅
    """

    def __init__(self, llm, req_info: EntityWordDetectRequestInfo, 
                 ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                 prev_prdct_svc_word_dict: Optional[Dict[str, Set[str]]] = {},
                 prev_pfrm_qalty_word_dict: Optional[Dict[str, Set[str]]] = {},
                 prompt_path: Optional[str] = default_resource_path + '/prompt') -> None:

        self.req_info: EntityWordDetectRequestInfo = req_info

        # 1. llm Chat 모델 인스턴스 생성
        self.llm = llm
        self.failback_llm = llm
        self.json_parser = JsonOutputParser()

        # 2. 시스템 프롬프트 세팅
        self.load_resources(prompt_path)

        # 3. 조사채널, 고객경험단계 별 고객경험요소 Dict
        self.ch_stge_cxe_dict = ch_stge_cxe_dict

        # 4. 이전 실행 결과 용어 Dict
        # { '채널코드_고객경험요소코드' : Set[str], ... }
        self.prev_prdct_svc_word_dict = prev_prdct_svc_word_dict
        self.prev_pfrm_qalty_word_dict = prev_pfrm_qalty_word_dict


    def load_resources(self, prompt_path: str) -> None:
        """
        system prompt loading
        """

        # 개체어 식별 프롬프트 (고객경험요소 유)
        system_prompt_cxe_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_CXE_FILE_NAME)
        system_prompt_cxe_formatted_str = system_prompt_cxe_str.format(
            RESULT_KEY=CommonCode.RESULT_KEY.value,
            PRODUCT_SERVICE_KEY=PRODUCT_SERVICE_KEY,
            PERFORMANCE_QUALITY_KEY=PERFORMANCE_QUALITY_KEY
        )
        self.system_cxe_prompt: SystemMessage = SystemMessage(content=system_prompt_cxe_formatted_str)
        self.human_cxe_prompt_str: str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_CXE_FILE_NAME)
        
        # 개체어 식별 프롬프트 (고객경험요소 무)
        system_prompt_no_cxe_str: str = load_resource_file(prompt_path + "/" + SYSTEM_PROMPT_NO_CXE_FILE_NAME)
        system_prompt_no_cxe_formatted_str = system_prompt_no_cxe_str.format(
            RESULT_KEY=CommonCode.RESULT_KEY.value,
            PRODUCT_SERVICE_KEY=PRODUCT_SERVICE_KEY,
            PERFORMANCE_QUALITY_KEY=PERFORMANCE_QUALITY_KEY
        )
        self.system_no_cxe_prompt: SystemMessage = SystemMessage(content=system_prompt_no_cxe_formatted_str)
        self.human_no_cxe_prompt_str: str = load_resource_file(prompt_path + "/" + HUMAN_PROMPT_NO_CXE_FILE_NAME)

    
    def load_langfuse_resources(self) -> None:
        return


    async def execute(self, voc_infos: List[VocInfo]) -> List[VocInfo]:
        """
        [1] 최 상위 호출 메서드
        ===========================================
        1. '채널_고객경험요소'를 Key로 추출대상 voc들을 구룹화 (같은 고객경험요소들끼리는 한꺼번에 Context로 입력되기 때문에)
        2. NLP - 개체어 식별 생성 (비동기)
        3. Output Validation and Setting
        4. Return
        """

        # 1. 채널 / 고객경험요소로 sort & 분리 (미분류 포함)
        grouped_voc_list = self.group_voc_list(voc_infos)
        
        # 2. 개체어 식별 생성
        # batch 사이즈만큼 Chunk 단위로 비동기 실행
        batch_size = self.req_info.batch_size

        chunk_index = 0
        for i in range(0, len(grouped_voc_list), batch_size):
            voc_chunk_list = grouped_voc_list[i:i + batch_size]

            tasks = []
            for voc_chunk in voc_chunk_list:
                tasks.append(self.generate_entity_word_detect_wrap(voc_chunk, chunk_index))
                chunk_index = chunk_index + 1
            
            # gather() 에는 * 연산자를 통해 인자 풀기
            await asyncio.gather(*tasks)
            await asyncio.sleep(self.req_info.batch_sleep)

        return voc_infos


    def group_voc_list(self, voc_list: List[VocInfo]) -> List[List[VocInfo]]:
        """
        [2] '채널_고객경험요소'를 Key로 VOC 그룹화
        =================================================
        1. 채널 / 고객경험요소 로 Grouping
        2. 같은 그룹에서도 Chunk Size만큼 Chunking (Chunk: 한번에 LLM요청에 입력된 VOC의 갯수)
        """
        
        grouped = defaultdict(list)

        # 1. 채널 / 고객경험요소 로 Grouping
        for voc in voc_list:
            if voc.cxe_success_yn:
                # 1-1. 고객경험요소 분류 VOC, 고객경험요소를 키값으로 분류
                rep_cxe = voc.cxe_result[0]
                key = f"{voc.final_qsitm_pol_taget_dstcd}_{rep_cxe.cxe_nm}"
            else:
                # 1-2. 고객경험요소 미분류 VOC, 미분류 키값으로 분류
                key = CommonCode.NO_CXE_ID.value
            
            grouped[key].append(voc)
        grouped_list: List[List[VocInfo]] = grouped.values()

        # 2. 같은 그룹에서도 Chunk Size만큼 Chunking (Chunk: 한번에 LLM요청에 입력된 VOC의 갯수)
        chunk_size = self.req_info.voc_chunk_size
        chunked_list: List[List[VocInfo]] = []
        for g_list in grouped_list:
            chunked = [g_list[i:i + chunk_size] for i in range(0, len(g_list), chunk_size)]
            chunked_list.extend(chunked)
        
        return chunked_list
        
        

    async def generate_entity_word_detect_wrap(self, voc_list: List[VocInfo], chunk_index: int) -> None:
        """
        [3] Chunk 사이즈 별 개체어 식별 매핑 wrapping
        ==================================
        실제 실행 부를 감싸고 있는 wrapping 메서드
        - logging
        - try-catch
        """

        # 1. 개체어 식별 시작 시간
        generate_start_time = time.time()

        try:
            # 2-1. 실제 고객경험요소 매핑 실행 부분
            await self.generate_entity_word_detect_and_retry(voc_list, chunk_index)
                        
        except Exception as e:
            logger.exception(f"에러 발생 -> SKIP Chunk: [index: {chunk_index}, size: {len(voc_list)}]")
            
        finally:
            # 3. 개체어 식별 종료 시간
            generate_end_time = time.time()
            elapsed_time = generate_start_time - generate_end_time


    async def generate_entity_word_detect_and_retry(self, voc_list: List[VocInfo], chunk_index: int) -> None:
        """
        [4] Chunk 별 개체어 식별 및 재시도
        =============================================
        1. 개체어 식별 및 결과 세팅
        2. 만약 입력한 voc중 LLM이 응답에 빠뜨린 voc가 있다면, 누락된 voc만 재시도 (기본 3회)
            - 입력한 모든 voc가 개체어 추출이 되어야 한다. (추출할 개체어가 없더라도, 결과에는 포함되어 있어야 함)
        """

        retry_cnt = 0
        
        # 1. generate 개체어 식별
        retry_voc_list: List[VocInfo] = await self.generate_entity_word_detect(voc_list, chunk_index, retry_cnt)
        
        # 2. 재시도 Limit 만큼 재시도
        while retry_cnt < self.req_info.retry_limit:
            # 재시도할 voc가 없으면 break
            if not retry_voc_list:
                break
            
            retry_cnt = retry_cnt + 1
            await self.generate_entity_word_detect(retry_voc_list, chunk_index, retry_cnt)
        
        return


    async def generate_entity_word_detect(self, voc_list: List[VocInfo], chunk_index: int, retry_cnt: int) -> List[VocInfo]:
        """
        [4-1] Chunk 사이즈 별 개체어 식별 실제 실행 부
        =================================================
        1. 하나의 청크를 하나의 프롬프트로 세팅
        2. NLP - 개체어 추출
        3. 추출 결과 Valid and 누락된 retry voc 배열 반환
        4. retry voc 배열 반환
        """

        # 1. generate 개체어 식별 result
        generated_response = await DataAnalysisUtils.ainvoke_llm(self.get_prompt(voc_list), self.llm, self.failback_llm)

        # 2. result setting
        retry_voc_list = self.valid_and_set_result(voc_list, generated_response.content)
        
        return retry_voc_list



    def valid_and_set_result(self, voc_list: List[VocInfo], gen_content: str) -> List[VocInfo]:
        """
        [4-1-2] 추출 결과 validation and setting
        =================================================
        1. 응답의 형식이 올바른지 Valid And Parsing
        2. 모든 VOC의 개체어를 생성 했는지 Valid -> 빠진 Voc는 재시도를 위해 반환
        3. 생성된 개체어 이전 실행결과에 추가
        4. voc_info에 생성된 결과값 추가
        """

        # 1. 생성 결과 검증
        if not gen_content:
            # 생성된 응답이 없으면 -> 전체 재시도
            return voc_list
            
        # 2. JSON 결과값 Parsing
        parsed_content: Dict = self.json_parser.parse(gen_content)

        # 3. 개체어 식별 결과 검증
        entity_word_dict = parsed_content[CommonCode.RESULT_KEY.value]
        if not entity_word_dict or not isinstance(entity_word_dict, dict):
            # 3-1. 결과값이 없거나, List 형태가 아닌경우 -> 전체 재시도
            return voc_list

        for idx_str, result in entity_word_dict.items():
            # 3-2. 알맞는 VOC에 개체어 적재
            self.set_result(idx_str, result, voc_list)

        # 4. 적재되지 않은 VOC 반환
        return [
            voc_info
            for voc_info in voc_list
            if not voc_info.cxe_result[0].detect_success_yn
        ]


    
    def set_result(self, idx_str: str, result: Dict[str, str], voc_list: List[VocInfo]) -> None:
        """
        [4-1-2-1] 성공한 추출 결과 setting
        =================================================
        1. 대표 고객경험요소 결과에 개체어를 각각 적재
        2. 이전 개체어 결과 셋에 이번 개체어 결과를 추가
        """

        
        try:
            voc_idx: int = int(idx_str)
            voc_info = voc_list[voc_idx]

            # 1. 대표 고객경험요소에 개체어 적재
            rep_cxe = voc_info.cxe_result[0]
            
            prdct_svc_word = result.get(PRODUCT_SERVICE_KEY, "")
            pfrm_qalty_word = result.get(PERFORMANCE_QUALITY_KEY, "")

            rep_cxe.detect_success_yn = True
            rep_cxe.prdct_svc_word = prdct_svc_word
            rep_cxe.pfrm_qalty_word = pfrm_qalty_word

            
            # 2. 이전 개체어 set에 이번 결과 저장 (다음 번 실행에 활용하기 위해)
            prev_set_key = DataAnalysisUtils.get_prev_word_set_key( # 이전 개체어 Dict의 Key로 쓰일 값 세팅 ('채널_고객경험요소')
                voc_info.final_qsitm_pol_taget_dstcd, 
                rep_cxe.cxe_cd
            )

            # 2-1. 이전 상품서비스용어 결과 Set이 있다면 -> 해당 셋에 이번 결과 개체어 add 
            #                                     없으면 -> 신규 Set 초기화해서 개체어 add
            if prdct_svc_word:
                prev_dict = self.prev_prdct_svc_word_dict.get(prev_set_key)
                if prev_dict:
                    prev_dict.add(prdct_svc_word)
                else:
                    # 신규 셋 추가
                    self.prev_prdct_svc_word_dict[prev_set_key] = set([prdct_svc_word])
                    
            # 2-1. 이전 성능품질용어 결과 Set이 있다면 -> 해당 셋에 이번 결과 개체어 add 
            #                                   없으면 -> 신규 Set 초기화해서 개체어 add
            if pfrm_qalty_word:
                prev_dict= self.prev_pfrm_qalty_word_dict.get(prev_set_key)
                if prev_dict:
                    prev_dict.add(pfrm_qalty_word)
                else:
                    # 신규 셋 추가
                    self.prev_pfrm_qalty_word_dict[prev_set_key] = set([pfrm_qalty_word])
            
        except:
            # 인덱스로 변환되지 않는 문자열 / 실제 없는 인덱스 -> 환각은 재시도
            return
        
        
    def get_prompt(self, voc_list: List[VocInfo]) -> List[BaseMessage]:
        """
        [4-1-1] 고객경험요소 분류 성공 여부에 따라 프롬프트 세팅
        ==============================================================
        1. System Prompt와 Voc별 필요한 정보 Prompt 세팅
            - voc의 ID는 단순화를 위해 ArrayList의 Index로 대체
            
        2. 해당 '채널_고객경험요소' 이전 실행 개체어 결과 Context 세팅
        """

        # 1. 대표 VOC - 채널 / 고객경험요소 별로 이미 분류되어 있기 때문에
        rep_voc = voc_list[0]

        # 2. 고객경험요소 분류 / 미분류 여부에 따라 Context 값 세팅
        rep_cxe_cd: Optional[str] = None
        system_prompt: Optional[SystemMessage] = None
        human_prompt_str: Optional[str] = None
        cxe_ascii_table: Optional[str] = None

        # 2-1. 고객경험요소 분류 청크 일 경우
        if rep_voc.cxe_success_yn:
            # 대표 고객경험요소 ID 세팅
            rep_cxe_cd = rep_voc.cxe_result[0].cxe_cd
            # 시스템 프롬프트 세팅 (고객경험요소 유)
            system_prompt = self.system_cxe_prompt
            # 휴먼 프롬프트 세팅 (고객경험요소 유)
            human_prompt_str = self.human_cxe_prompt_str
            # 고객경험요소 사전 세팅
            cxe_ascii_table: str = self.get_cxe_ascii_table(rep_voc.final_qsitm_pol_taget_dstcd,
                                                            rep_voc.cx_stge_dstcd,
                                                            rep_cxe_cd)
        # 2-1. 고객경험요소 미분류 청크 일 경우
        else:
            # 대표 고객경험요소 ID 세팅
            rep_cxe_cd = CommonCode.NO_CXE_ID.value
            # 시스템 프롬프트 세팅 (고객경험요소 무)
            system_prompt = self.system_no_cxe_prompt
            # 휴먼 프롬프트 스트링 세팅 (고객경험요소 무)
            human_prompt_str = self.human_no_cxe_prompt_str
            
            
        # 3. 이전 실행 개체어 결과 Context 세팅
        prev_set_key = DataAnalysisUtils.get_prev_word_set_key(
            rep_voc.final_qsitm_pol_taget_dstcd, 
            rep_cxe_cd
        )
        prev_prdct_svc_word_list: List[str] = self.get_prev_result(self.prev_prdct_svc_word_dict.get(prev_set_key))
        prev_pfrm_qalty_word_list: List[str] = self.get_prev_result(self.prev_pfrm_qalty_word_dict.get(prev_set_key))

        # 4. VOC Input Context 세팅
        voc_list_input: str = self.get_voc_list_input(voc_list)

        # 5. 프로프트 최종 세팅 및 반환
        human_prompt = HumanMessage(content=human_prompt_str.format(
            prev_prdct_svc_word_list=prev_prdct_svc_word_list,
            prev_pfrm_qalty_word_list=prev_pfrm_qalty_word_list,
            cxe_ascii_table=cxe_ascii_table,
            voc_list_input=voc_list_input
        ))
        return [system_prompt, human_prompt]  

    

    def get_cxe_ascii_table(self, ch_cd: str, cx_stge_cd: str , cxe_cd: str) -> str:
        """
        [4-1-1-1] 고객경험요소 분류 성공 청크일 경우, Context로 첨부할 고객경험요소 사전 세팅
        ========================================================
        1. 해당 청크의 채널의 고객경험요소 사전을 Get
        2. Context에 첨부할 String 세팅
        3. 반환
        """
        
        # 1. 고객경험요소 사전 get
        final_cxe_dict = DataAnalysisUtils.get_final_cxe_dict(
            self.ch_stge_cxe_dict[ch_cd],
            cx_stge_cd
        )

        # 2. Context 세팅
        final_cxe_meta = final_cxe_dict.get(cxe_cd, None)
        context:str = ""
        if final_cxe_meta:
            context = f"""
            | 고객경험요소 | 설명 |
            |--------------|------|
            | {final_cxe_meta.get(CommonCode.CXE_NAME.value)} | {final_cxe_meta.get(CommonCode.CXE_DESC.value)} |
            """
        return context

    
    def get_voc_list_input(self, voc_list: List[VocInfo]) -> str:
        """
        [4-1-1-2] 청크의 모든 Voc를 Input Context로 세팅
        ========================================================
        1. 청크의 모든 Voc를 Input Context로 세팅
        2. 반환
        """
        
        voc_rows = []
        voc_idx = 0
        for voc in voc_list:
            voc_row = f"""
            ========[VOC ID: {voc_idx}]========
            VOC원문: {voc.orin_voc_content}
            
            """.strip()
            voc_rows.append(voc_row)
            voc_idx = voc_idx + 1

        return "\n".join(voc_rows)


        
    def get_prev_result(self, prev_set: Set[str]) -> List[str]:
        """
        [4-1-1-3] 청크의 채널/고객경험요소에 해당하는 이전 실행 개체어 결과를 prev_word_input_size만큼 슬라이싱해서 반환한다.
        ========================================================
        1. prev_set 배열로 convert
        2. prev_word_input_size 만큼 끝에서부터 슬라이싱
        3. 반환
        """
        
        if not prev_set:
            return []
        
        set_list = list(prev_set)

        # 마지막 인덱스부터 i개 가져오기
        input_size = self.req_info.prev_word_input_size
        return set_list[-input_size:]
        
    