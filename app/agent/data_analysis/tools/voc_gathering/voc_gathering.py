import os
import asyncio
from pathlib import Path
import re
import itertools
import time

import pandas as pd
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Set

from core.util import load_resource_file
from core.mcp_util import get_mcp_executor
from core.logger import get_logger

from agent.data_analysis.utils import DataAnalysisUtils
from agent.data_analysis.model.vo import VocInfo, EmotionInfo
from agent.data_analysis.model.consts import CommonCode, OfferExcludeMessage


# 상수 - 쿼리
MYSQL_QUERY_LIMIT = 500


# resources 기본 경로 설정
default_resource_path = str(Path(__file__).resolve().parents[2] / 'resources')

# 리소스
TOTAL_COUNT_SQL_FILE_NAME = f"sql/gathering_voc_total_count.sql"
GATHERING_SQL_FILE_NAME = f"sql/gathering_voc_response.sql"
EMOTION_SQL_FILE_NAME = f"sql/select_emotion.sql"
EXCEPTION_WORDS_SQL_FILE_NAME = f"sql/select_exception_words.sql"
CXE_STANDARD_SQL_FILE_NAME = f"sql/select_cxe_standard.sql"
PREV_WORD_SQL_FILE_NAME = f"sql/select_prev_entity_word.sql"

logger = get_logger("voc_gatherer")



class VocGatherer:
    """
    Voc Gatherer (VOC 수집):
    일처리 대상 voc 중 데이터 분석 대상 voc 수집
    ===========================================
    기능:
    1. 입력된 기준일자에 해당하는 분석 대상 VOC List
    2. 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict[ch, Dict[stge, Dict[cxe_cd, cxe_meta_dict]]]]
    3. 채널 별, 고객경험단계 리스트 Dict
    4. 감정 정보 Dict
    5. 이전 실행시, 분류된 개체어 Dict[str, Set[str]] `상품서비스용어`, `성능품질용어` 각각 한개씩
    6. VOC의 제공/제외 여부 세팅
    7. 분석 대상 VOC total count
    """

    def __init__(self, page: int, page_size: int, start_ymd: str, end_ymd: Optional[str] = None, 
                 pol_mod_cd_list: Optional[List[str]] = None, ch_cd_list: Optional[List[str]] = None,
                 mcp_executor = None, prev_word_size: int = 0) -> None:

        # 수집 대상 페이지
        self.page = page
        self.page_size = page_size

        # 수집 대상 일자
        self.start_ymd = start_ymd
        self.end_ymd = end_ymd if end_ymd else start_ymd # 종료일자가 없으면, start_ymd 하루

        # 수집 대상 설문 방식 / 채널
        self.pol_mod_cd_list = pol_mod_cd_list
        self.ch_cd_list = ch_cd_list

        # 이전 실행 결과(개체어) 사이즈
        self.prev_word_size = prev_word_size

        # 쿼리 파일 load
        self.load_resources()

        # mcp executor
        self.mcp_executor = mcp_executor

    def load_resources(self) -> None:
        """
        resources loading
        """
        # VOC 수집 쿼리
        self.voc_total_count_query = load_resource_file(default_resource_path + "/" + TOTAL_COUNT_SQL_FILE_NAME)
        self.voc_gathering_query = load_resource_file(default_resource_path + "/" + GATHERING_SQL_FILE_NAME)
        
        # 감정 쿼리
        self.emotion_query = load_resource_file(default_resource_path + "/" + EMOTION_SQL_FILE_NAME)

        # 불용어 사전 쿼리
        self.exception_words_query = load_resource_file(default_resource_path + "/" + EXCEPTION_WORDS_SQL_FILE_NAME)
        
        # 고객경험요소 기본 쿼리
        self.cxe_standard_query = load_resource_file(default_resource_path + "/" + CXE_STANDARD_SQL_FILE_NAME)

        # 이전 실행 개체어 결과 조회 쿼리
        self.prev_word_query = load_resource_file(default_resource_path + "/" + PREV_WORD_SQL_FILE_NAME)

        
    async def init_mcp_executor(self):
        if not self.mcp_executor:
            self.mcp_executor = await get_mcp_executor()

    
    async def execute(self) -> Tuple[
        List[VocInfo], 
        Dict[str, Dict[str, Dict[str, Dict[str, str]]]], 
        Dict[str, List[str]], 
        Dict[str, EmotionInfo], 
        Dict[str, Set[str]], 
        Dict[str, Set[str]],
        int,
    ]:
        """
        실행 부
        =====================
        Response
        1. 입력된 기준일자에 해당하는 분석 대상 VOC List
        2. 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict[ch, Dict[stge, Dict[cxe_cd, cxe_meta_dict]]]]
        3. 채널 별, 고객경험단계 리스트 Dict
        4. 감정 정보 Dict
        5. 이전 실행시, 분류된 개체어 Dict[str, Set[str]] `상품서비스용어`, `성능품질용어` 각각 한개씩
        6. VOC의 제공/제외 여부 세팅
        7. 분석 대상 VOC total count
        """

        # 0. MCP executor 캐싱
        await self.init_mcp_executor()
        
        # 1. VOC 수집 and total count
        voc_list = await self.gather_voc_list()
        if len(voc_list) == 0:
            return ([], {}, {}, {}, {}, {}, 0)

        # 2, 3. 고객경험요소 Dict 세팅
        ch_stge_cxe_dict, ch_stge_nm_dict = await self.read_ch_stge_cxe_dict()
    
        # 4. 감정 정보 조회 및 Dict 세팅
        emotion_dict = await self.get_emotion_info()

        # 5. 이전 결과 개체어 조회 및 Dict 세팅
        prev_prdct_svc_word_dict, prev_pfrm_qalty_word_dict = await self.get_prev_word_result_set()

        # 6. 제공/제외 여부 세팅
        await self.set_offer_yn(voc_list)

        # 7. total count
        total_count = await self.get_voc_total_count()

        return ( 
            voc_list, 
            ch_stge_cxe_dict, 
            ch_stge_nm_dict, 
            emotion_dict, 
            prev_prdct_svc_word_dict, 
            prev_pfrm_qalty_word_dict,
            total_count
        )
        
    async def gather_voc_list(self) -> List[VocInfo]:
        """
        1. 기준년월일을 기준으로 분석 대상 VOC 조회 및 VO 세팅
        """

        # 조사 방식 구분코드 조건 세팅 (없으면, 'TD', 'BU' 모두)
        final_pol_mod_cd_list = "IS NOT NULL"
        if self.pol_mod_cd_list:
            final_pol_mod_cd_list = "IN ( '" + "','".join(self.pol_mod_cd_list) + "' )"

        # 문항설문조사대상구분코드 조건 세팅 (없으면, 모든 채널)
        final_ch_cd_list = "IS NOT NULL"
        if self.ch_cd_list:
            final_ch_cd_list = "IN ( '" + "','".join(self.ch_cd_list) + "' )"


        # 페이징 조회 (현재 Mysql 기본 LIMIT 500)
        voc_list: List[VocInfo] = []
        total_offset = self.page * self.page_size  # 전체 데이터에서의 시작 위치
        remaining_records = self.page_size  # 가져와야 할 남은 레코드 수

        while remaining_records > 0:
            # 현재 쿼리에서 가져올 레코드 수 계산
            current_limit = min(MYSQL_QUERY_LIMIT, remaining_records)
            rn_start = total_offset + 1
            rn_end = total_offset + current_limit

            # 현재 쿼리 포맷팅
            current_query = self.voc_gathering_query.format(
                start_ymd=self.start_ymd,
                end_ymd=self.end_ymd,
                pol_mod_param=final_pol_mod_cd_list,
                chnl_param=final_ch_cd_list,
                rn_start=rn_start,
                rn_end=rn_end,
            )
            
            logger.info(f"[VOC 수집] 쿼리 (페이지: {self.page}, 오프셋 {total_offset})") # :\n{current_query}

            # 실제 조회
            search_result = await self.mcp_executor.execute_tool("mysql_query", {"query": current_query})

            # 더 이상 결과가 없으면 종료
            if isinstance(search_result, str):
                raise ValueError(f"조회 에러 발생: {search_result}")
            
            if not isinstance(search_result, list) or len(search_result) == 0:
                break

            logger.info(f"[VOC 수집] 페이지: {self.page}, 결과 수: {len(search_result)}")
            for row in search_result:
                voc_list.append(self.convert_row_to_voc_info(row))

            # 다음 쿼리를 위한 값 업데이트
            total_offset += current_limit
            remaining_records -= current_limit
        
        return voc_list


    async def read_ch_stge_cxe_dict(self) -> Tuple[Dict[str, Dict[str, Dict[str, Dict[str, str]]]], Dict[str, List[str]]]:
        """
        2, 3. CXE 정보 읽어오기
        """
        # -------------------------------------------------
        # 1️⃣ 고객경험요소 데이터 읽어오기
        # -------------------------------------------------
        search_result = await DataAnalysisUtils.mcp_pagination(self.mcp_executor, "mysql_query", self.cxe_standard_query)
        logger.info(f"[VOC 수집] 고객경험요소 기본 쿼리 (결과 수: {len(search_result)})") # :\n{self.cxe_standard_query}
        df = pd.DataFrame(search_result)
        
        # 현재 요청 정보에 따라 필터링
        # 1. 설문 조사 방식으로 필터링 
        if self.pol_mod_cd_list:
            df = df[df['설문조사방식구분코드'].isin(self.pol_mod_cd_list)]

        # 2. 채널로 필터링
        if self.ch_cd_list:
            df = df[df['설문조사대상구분코드'].isin(self.ch_cd_list)]
        
    
        # -------------------------------------------------
        # 2️⃣ “고객경험요소”‑> {sq, desc} 로 변환하는 헬퍼
        # -------------------------------------------------
        def _elem_dict(g: pd.DataFrame) -> Dict[str, Dict[str, str]]:
            """
            g : 한 고객경험단계구분코드에 속한 서브 DataFrame
            반환값 : {
                "고객경험요소코드": {
                    "cxe_nm": 고객경험요소명,
                    "cxe_desc": 관리목표,
                    "sq_cd": 서비스품질요소코드, 
                    "sq_nm": 서비스품질요소명, 
                },
                ...
            }
            """
            # 인덱스를 고객경험요소로 잡고, 두 컬럼만 남긴 뒤 이름을 바꾼다.
            return (
                g.set_index('고객경험요소코드')
                 [['고객경험요소명', '관리목표', '서비스품질요소코드', '서비스품질요소명']]
                 .rename(columns={
                     '고객경험요소명': CommonCode.CXE_NAME.value,
                     '관리목표': CommonCode.CXE_DESC.value,
                     '서비스품질요소코드': CommonCode.SQ_CD.value,
                     '서비스품질요소명': CommonCode.SQ_NAME.value
                 })
                 .to_dict(orient='index')
            )
    
        # -------------------------------------------------
        # 3️⃣ 채널 별로 한 번 더 그룹화 → 최종 딕셔너리 구성
        # -------------------------------------------------
        ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
        for channel, df_channel in df.groupby('설문조사대상구분코드'):
            # 각 채널 안에서 고객경험단계구분코드 별로 _elem_dict 적용
            nested_by_code = (
                df_channel.groupby('고객경험단계구분코드')
                          .apply(_elem_dict)
                          .to_dict()
            )
            ch_stge_cxe_dict[channel] = nested_by_code
    
        ch_stge_nm_dict: Dict[str, List[str]] = (
            df.groupby('설문조사대상구분코드')["고객경험단계구분명"]
              .unique() 
              .apply(list)
              .to_dict()
        )

        return ch_stge_cxe_dict, ch_stge_nm_dict
        

    async def get_emotion_info(self) -> Dict[str, EmotionInfo]:
        """
        4. 감정 중분류 - 감정 대분류 - VOC 유형 매핑 Dict 세팅
        """

        # 고객경험정답감정코드 조회
        search_result = await self.mcp_executor.execute_tool("mysql_query", {"query": self.emotion_query})
        logger.info(f"[VOC 수집] 감정분류 쿼리 (결과 수: {len(search_result)})") # :\n{self.emotion_query}
        
        emotion_dict: Dict[str, EmotionInfo] = {}
        for row in search_result:
            emotion_info = self.convert_row_to_emotion_info(row)
            emotion_dict[emotion_info.emtn_mid_cd] = emotion_info

        return emotion_dict

    
    async def get_prev_word_result_set(self) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        5. 이전 식별된 개체어 조회 및 Set으로 세팅
        """

        prev_prdct_svc_word_dict: Dict[str, Set[str]] = {}
        prev_pfrm_qalty_word_dict: Dict[str, Set[str]] = {}
        
        # 1. 이전 실행 개체어 조회 (현재는 개체어 분류 용어 사전이 없음)
        final_query = self.prev_word_query.format(prev_word_size=self.prev_word_size)

        search_result = await DataAnalysisUtils.mcp_pagination(self.mcp_executor, "mysql_query", final_query)
        logger.info(f"[VOC 수집] 이전 개체어 결과 조회 쿼리 (결과 수: {len(search_result)})") # :\n{final_query}
        
        if isinstance(search_result, str):
            logger.warning(f"[VOC 수집] 이전 개체어 결과 조회 실패 SKIP: {search_result}")
            return prev_prdct_svc_word_dict, prev_pfrm_qalty_word_dict
        
        # 2. 이전 실행 개체어 결과 SET 세팅
        for row in search_result:
            # 2-1. 이전 실행 `상품서비스용어`, `성능품질용어` Set의 키 포맷 '{ch}_{cxe_cd}'
            # -> 미분류는 CommonCode.NO_CXE_ID.value로 'cxe_cd'
            prev_set_key = DataAnalysisUtils.get_prev_word_set_key(row['CHNL'], row['CXE_CD'])
            prev_prdct_svc_word_set = prev_prdct_svc_word_dict.get(prev_set_key)
            prev_pfrm_qalty_word_set = prev_pfrm_qalty_word_dict.get(prev_set_key)
            word = row['WORD']
            
            # 2-2. 상품서비스용어일 경우
            if row['PRODUCT_SERVICE_YN'] == '1':
                if not prev_prdct_svc_word_set:
                    # 신규 셋 추가
                    prev_prdct_svc_word_dict[prev_set_key] = set([word])
                else:
                    # 기존 셋에 add
                    prev_prdct_svc_word_set.add(word)
            # 2-3. 성능품질용어일 경우
            else:
                if not prev_pfrm_qalty_word_set:
                    # 신규 셋 추가
                    prev_pfrm_qalty_word_dict[prev_set_key] = set([word])
                else:
                    # 기존 셋에 add
                    prev_pfrm_qalty_word_set.add(word)
        
        return prev_prdct_svc_word_dict, prev_pfrm_qalty_word_dict


    async def set_offer_yn(self, voc_list: List[VocInfo]) -> None:
        """
        6. VOC의 제공/제외 여부 세팅
        - 20음절 이하 (직원/부점장 모두 미제공)
        - 불용어 포함 (직원/부점장 모두 미제공) ('영업점(07)'과 '고객센터(08)')
        - 고객경험단계에 따라 여부 체크 (직원 미제공) ('영업점(07)'과 '고객센터(08)')
        """

        # 1. 불용어 사전 조회
        search_result = await self.mcp_executor.execute_tool("mysql_query", {"query": self.exception_words_query})
        logger.info(f"[VOC 수집] 불용어사전 쿼리 (결과 수: {len(search_result)})") # :\n{self.exception_words_query}
        except_words_set = set([row['고객접점유사단어명'] for row in search_result])

        # 2. 제공/제외 여부 판별
        for voc_info in voc_list:

            brnmgr_ofer_yn = True
            emp_ofer_yn = True
            exclude_reason = []
            
            # 2-1. 20음절 체킹
            voc_len = len(voc_info.orin_voc_content.replace(" ", ""))
            if voc_len <= 20:
                brnmgr_ofer_yn = False
                emp_ofer_yn = False
                exclude_reason.append(OfferExcludeMessage.WORD_LENGTH.value)

            # 2-2. 영업점 / 고객센터 제공/제외 여부 판별
            if voc_info.final_qsitm_pol_taget_dstcd in [ '07', '08' ]:
                # 2-2-1. 불용어 여부 판별
                include_except_word = any(except_word in voc_info.orin_voc_content for except_word in except_words_set)
                if include_except_word:
                    brnmgr_ofer_yn = False
                    emp_ofer_yn = False
                    exclude_reason.append(OfferExcludeMessage.EXCEPT_WORD_INCLUDE.value)
                # 2-2-2. 특정 고객경험단계 분기 (04 - 맞이/의도파악, 15 - 직원상담, 06 - 업무처리/배웅)
                if voc_info.cx_stge_dstcd in [ '04', '15', '06' ]:
                    emp_ofer_yn = False

            # 3. 제공/제외 사유 세팅
            exclude_reason_str = "/".join(exclude_reason) if exclude_reason else None

            # 4. 최종 결과 세팅
            voc_info.brnmgr_ofer_yn = brnmgr_ofer_yn
            voc_info.emp_ofer_yn = emp_ofer_yn
            voc_info.text_eclud_resn_ctnt = exclude_reason_str
        
    async def get_voc_total_count(self) -> int:
        """
        7. 조회 대상 VOC Total count 조회
        """

        # 1. 조사 방식 구분코드 조건 세팅 (없으면, 'TD', 'BU' 모두)
        final_pol_mod_cd_list = "IS NOT NULL"
        if self.pol_mod_cd_list:
            final_pol_mod_cd_list = "IN ( '" + "','".join(self.pol_mod_cd_list) + "' )"

        # 2. 문항설문조사대상구분코드 조건 세팅 (없으면, 모든 채널)
        final_ch_cd_list = "IS NOT NULL"
        if self.ch_cd_list:
            final_ch_cd_list = "IN ( '" + "','".join(self.ch_cd_list) + "' )"

        # 3. 현재 쿼리 포맷팅
        current_query = self.voc_total_count_query.format(
            start_ymd=self.start_ymd,
            end_ymd=self.end_ymd,
            pol_mod_param=final_pol_mod_cd_list,
            chnl_param=final_ch_cd_list,
        )
        
        # 4. Total Count 조회
        search_result = await self.mcp_executor.execute_tool("mysql_query", {"query": current_query})
        total_count = search_result[0]['total_elements']
        logger.info(f"[VOC 수집] Total Count 쿼리 (Total: {total_count})") # :\n{current_query}

        return total_count
    

    #========================================================#
    #               Converting Utility Methods               #
    #========================================================#
    def convert_row_to_emotion_info(self, row: Dict[str, Any]) -> EmotionInfo:

        emotion_info = EmotionInfo(
            emtn_mid_cd=row['고객경험정답감정코드'],
            emtn_mid_nm=row['고객경험정답감정명'],
            emtn_lag_cd=row['고객감정대분류구분'],
            emtn_lag_nm=row['고객감정대분류구분명'],
            voc_typ_cd=row['고객경험VOC유형구분'],
            voc_typ_nm=row['고객경험VOC유형구분명'],
        )
        return emotion_info
        

    def convert_row_to_voc_info(self, row: Dict[str, Any]) -> VocInfo:
        
        voc_info = VocInfo(
            # keys
            group_co_cd=row['그룹회사코드'],
            base_ymd=row['기준년월일'],
            qusn_id=row['설문ID'],
            qusn_invl_tagtp_uniq_id=row['설문참여대상자고유ID'],
            qsitm_id=row['문항ID'],
    
            # VOC
            orin_voc_content=row['문항응답내용'],
    
            # 설문 관련 데이터
            tran_bnk_dstcd=row['거래은행구분'],
            tran_bnk_dstic_nm=row['거래은행구분명'],
            pol_mod_dstcd=row['설문조사방식구분'],
            pol_mod_dstic_nm=row['설문조사방식구분명'],
            pol_knd_dstcd=row['설문조사종류구분'],
            cx_stge_dstcd=row['고객경험단계구분'],
            cx_stge_dstic_nm=row['고객경험단계구분명'],
            final_qsitm_pol_taget_dstcd=row['문항설문조사대상구분'],
            final_qsitm_pol_taget_nm=row['문항설문조사대상구분명'],
            qry_inten_lag_dstcd=row['질문의도대구분'],
            qry_inten_lag_nm=row['질문의도대구분명'],

            # 추천 이유
            rcmdn_rson_qsitm_id=row['추천이유문항ID'],
            rcmdn_resn_qsitm_name=row['추천이유문항제목'],
            rcmdn_resn_qsitm_desc=row['추천이유문항설명'],
            rcmdn_resn_qsitm_content=row['추천이유문항응답내용'],
            qs_sq_dstcd=row['문항서비스품질요소코드'],
        )
        return voc_info
    