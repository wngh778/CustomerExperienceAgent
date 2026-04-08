import asyncio, os
import time, pytz
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple, Set

from core.util import load_resource_file
from core.logger import get_logger
from core.config import settings
from agent.agent_template import Agent
from agent.data_analysis.tools import *
from agent.data_analysis.utils import DataAnalysisUtils

from agent.data_analysis.model.consts import CommonCode
from agent.data_analysis.model.vo import VocInfo, EmotionInfo, CxeInfo
from agent.data_analysis.model.dto import (
    DataAnalysisReqeustDto, VocFilterRequestInfo, CxeMapperRequestInfo,
    EmotionAnlaysisRequestInfo, EntityWordDetectRequestInfo, 
    VocProblemReasonDetectRequestInfo, ContentResponseDto, CxeResponseDto, 
    PageResponseDto, MetadataResponseDto, DataAnalysisResponseDto
)


logger = get_logger('data_analysis_agent')

# TPC 대응용 인스턴스 조회 SQL
INSTANCE_SQL_FILE_NAME = "select_instance.sql"

default_resource_path = "/".join(os.path.abspath(__file__).split("/")[:-1])

class DataAnalysisAgent(Agent):
    def __init__(self, prompt_path:str=default_resource_path+"/resources", 
                 tool_description_path:str=default_resource_path+"/tool_description"):
        
        super().__init__(prompt_path, tool_description_path)
        self.mcp_executor = None
        self.load_resources()


    def load_resources(self):
        # 인스턴스 select sql
        self.instance_sql = load_resource_file(default_resource_path + "/resources/sql/" + INSTANCE_SQL_FILE_NAME)


    def load_langfuse_resources(self):
        return


    async def execute(self, user_id: str, messages: list, today_date):
        pass


    async def execute_data_anlysis(self, request: dict, page_size:int=500, batch_size=15) -> DataAnalysisResponseDto:
        """
        데이터 분석 배치 실행
        ==============================
        Flow
        [1] VOC 수집
        [2] VOC 필터
        [3] CXE 매핑
        [4-1] 감정 분석
        [4-2] 개체어 식별
        [5] VOC 문제원인 식별 - 감정 대분류가 '부정'인 것만
        [6] 응답 DTO 세팅
        """
        
        # 0. 요청 DTO 세팅
        req_dto = DataAnalysisReqeustDto(
            agent_id="CX_AGENT",
            is_stream=False,
            page=request.get("page", 0),
            page_size=page_size,
            start_ymd=request.get("date"),
            end_ymd=request.get("date_end"), # end_ymd None이면, start_ymd 하루치만 실행
            batch_size=batch_size,
        )
        logger.info(f"[데이터 분석] 시작 [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, 페이지 사이즈: {req_dto.page_size}]")
        
        # 1. VOC 수집
        logger.info(f"[VOC 수집] 시작 [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}]")
        result = await self.execute_voc_gathering(req_dto)
        logger.info(f"[VOC 수집] 종료 [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, VOC 개수: {len(result[0])}]")

        if len(result[0]) != 0:
            # 2. VOC 필터
            logger.info(f"[VOC 필터] 시작: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, VOC 개수: {len(result[0])}]")
            filtered_voc_list = await self.execute_voc_filtering(req_dto, result[0], result[2])
            logger.info(f"[VOC 필터] 종료: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, 필터링되지 않은 VOC 개수: {len(filtered_voc_list)}]")

            # 3. CXE 매핑
            logger.info(f"[고객경험요소 매핑] 시작: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, 대상 VOC 개수: {len(filtered_voc_list)}]")
            await self.execute_cxe_mapping(req_dto, filtered_voc_list, result[1])
            logger.info(f"[고객경험요소 매핑] 종료: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}]")
            #================================================================================================#
            # 4-1. 감정 분석 (PTU RateLimitError 문제 때문에 동기적으로 실행)
            logger.info(f"[감정 분석] 시작: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, 대상 VOC 개수: {len(filtered_voc_list)}]")
            await self.execute_emotion_analysis(req_dto, filtered_voc_list, result[3], result[1])
            logger.info(f"[감정 분석] 종료: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}]")

            # 4-2. 개체어 식별 (PTU RateLimitError 문제 때문에 동기적으로 실행)
            logger.info(f"[개체어 식별] 시작: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, 대상 VOC 개수: {len(filtered_voc_list)}]")
            await self.execute_entity_word_detect(req_dto, filtered_voc_list, result[1], result[4], result[5])
            logger.info(f"[개체어 식별] 종료: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}]")
            #================================================================================================#
            # 4. 감정 분석 / 개체어 식별 동시 실행 (PTU RateLimitError 문제가 완화되면 비동기로 실행해도됨)
            #tasks = [
            #    self.execute_emotion_analysis(req_dto, filtered_voc_list, result[3], result[1]),
            #    self.execute_entity_word_detect(req_dto, filtered_voc_list, result[1], result[4], result[5])
            #]
            #await asyncio.gather(*tasks)
            #================================================================================================#

            # 5. VOC 문제원인 식별 (부정 VOC만)
            negative_voc_list = [
                voc_info
                for voc_info in filtered_voc_list
                if voc_info.emtn_success_yn and voc_info.emtn_result.emtn_lag_cd == CommonCode.NEGATIVE_EMOTION_LARGE_CD.value
            ]
            logger.info(f"[문제원인 식별] 시작: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}, 부정 VOC 개수: {len(negative_voc_list)}]")
            await self.execute_voc_problem_reason_detect(req_dto, negative_voc_list, result[1], result[2])
            logger.info(f"[문제원인 식별] 종료: [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}]")
            
            # 6. 응답 DTO 생성 및 반환
            response_dto = self.convert_vo_to_dto(req_dto, result[0], result[6])
        else:
            response_dto = self.convert_vo_to_dto(req_dto, [], 0)
        
        logger.info(f"[데이터 분석] 종료 [대상일자: {req_dto.start_ymd}, 페이지: {req_dto.page}]")
        return response_dto


    def convert_vo_to_dto(self, req_dto: DataAnalysisReqeustDto, voc_list: List[VocInfo], total_count: int) -> DataAnalysisResponseDto:
        """
        [6] 응답 DTO 세팅
        ===============================
        배치 실행 결과를 API 응답 DTO로 변환한다.
        """
        
        # 1. Content Dto 배열 세팅
        content_dtos = []
        if voc_list:
            for voc_info in voc_list:
                content_dtos.append(self.convert_vo_to_dto_cxe_info(req_dto.agent_id, voc_info))

        # 2. Page Dto 세팅
        number_of_elements = len(voc_list)
        first_yn = (req_dto.page == 0)
        last_yn = (number_of_elements == 0)

        # 3. total_pages 세팅
        if req_dto.page_size <= 0 or total_count <= 0:
            total_pages = 0
        else:
            total_pages = (total_count + req_dto.page_size - 1) // req_dto.page_size

        page_dto = PageResponseDto(
            page_number=req_dto.page,
            page_size=req_dto.page_size,
            total_elements=total_count,
            total_pages=total_pages,
            first_yn=first_yn,
            last_yn=last_yn,
            number_of_elements=number_of_elements,
            content=content_dtos,
        )

        # 4. Metadata Dto 세팅 
        meta_dto = MetadataResponseDto(
            prcss_mdel_vsnid=req_dto.agent_id,
            prcss_ym_yms=datetime.now(pytz.timezone("Asia/Seoul")).strftime('%Y%m%d%H%M%S'),
        )

        # 5. 응답 DTO 반환
        return DataAnalysisResponseDto(
            metadata=meta_dto,
            page=page_dto,
        )
        

    async def execute_voc_gathering(self, req_dto: DataAnalysisReqeustDto) -> Tuple[
                                        List[VocInfo], 
                                        Dict[str, Dict[str, Dict[str, Dict[str, str]]]], 
                                        Dict[str, List[str]], 
                                        Dict[str, EmotionInfo], 
                                        Dict[str, Set[str]], 
                                        Dict[str, Set[str]]
                                    ]:
        """
        [1] VOC 수집 - VOC 응답 및 이후 필요한 정보 세팅
        요청된 페이지만큼 VOC응답 데이터를 읽어온다.
        ===================
        @Return Index
        0. 입력된 기준일자에 해당하는 분석 대상 VOC List
        1. 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict[ch, Dict[stge, Dict[cxe_cd, cxe_meta_dict]]]]
        2. 채널 별, 고객경험단계 리스트 Dict
        3. 감정 정보 Dict
        4, 5. 이전 실행시, 분류된 개체어 Dict[str, Set[str]] `상품서비스용어`, `성능품질용어` 각각 한개씩
        6. 분석 대상 VOC total count
        """
        # 1. VOC 수집 객체 생성
        voc_gatherer = VocGatherer(
            page=req_dto.page,
            page_size=req_dto.page_size,
            start_ymd=req_dto.start_ymd,
            end_ymd=req_dto.end_ymd,
            pol_mod_cd_list=req_dto.pol_mod_cd_list,
            ch_cd_list=req_dto.ch_cd_list,
            mcp_executor=self.mcp_executor,
            prev_word_size=req_dto.prev_word_input_size,
        )

        # 2. VOC 수집 실행
        gathered_result = await voc_gatherer.execute()

        # 3. 반환
        return gathered_result

    
    async def execute_voc_filtering(self, req_dto: DataAnalysisReqeustDto, 
                                    voc_list: List[VocInfo], ch_stge_dict: Dict[str, List[str]]) -> List[VocInfo]:
        """
        [2] VOC 필터링
        VOC 필터링 기준
        1. VOC 중복 제거
        2. VOC 필터링(특수문자 & 자모 & 반복 & 10음절)
        3. NLP - VOC 필터링(채널 / 고객경험단계 무관)
        =================================================
        @Params
        ch_stge_dict - 채널 별 고객경험단계 리스트 Dict 
        """

        # 0. ReqDto 세팅
        voc_filter_dto = VocFilterRequestInfo(
            voc_length_limit=req_dto.voc_length_limit,
            batch_size = req_dto.batch_size,
            voc_chunk_size=req_dto.voc_chunk_size,
            batch_sleep=req_dto.batch_sleep,
        )
        
        # 1. VOC 필터 객체 생성
        voc_filter = VocFilter(
            llm=self.llm,
            req_dto=voc_filter_dto,
            ch_stge_dict=ch_stge_dict,
        )

        # 2. VOC 필터 실행
        await voc_filter.execute(voc_list)

        # 3. VOC되지 않은 VOC만 반환
        return [
            voc_info 
            for voc_info in voc_list
            if not voc_info.filtered_yn
        ]

    
    async def execute_cxe_mapping(self, req_dto: DataAnalysisReqeustDto, voc_list: List[VocInfo], 
                                  ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]]) -> None:
        """
        [3] CXE 매핑
        해당 VOC에 알맞는 고객경험요소 매핑
        =================================================
        @Params
        - voc_list: 필터링 되지 않은 VocList
        - ch_stge_cxe_dict: 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict
        """
        
        # 0. ReqDto 세팅
        cxe_mapper_dto = CxeMapperRequestInfo(
            batch_size=req_dto.batch_size,
            batch_sleep=req_dto.batch_sleep,
        )
        
        # 1. CXE 매핑 객체 생성
        cxe_mapper = CxeMapper(
            llm=self.llm,
            req_info=cxe_mapper_dto,
            ch_stge_cxe_dict=ch_stge_cxe_dict,
        )

        # 2. CXE 매핑 실행
        await cxe_mapper.execute(voc_list)
        return


    async def execute_emotion_analysis(self, req_dto: DataAnalysisReqeustDto,
                                       voc_list: List[VocInfo],
                                       emotion_dict: Dict[str, EmotionInfo],
                                       ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]]) -> None:
        """
        [4-1] 감정 분석
        VOC의 해당하는 감정을 분석한다.
        =================================================
        @Params
        - voc_list: 필터링 되지 않은 VocList
        - emotion_dict: 감정 중분류 - 감정 대분류 - VOC 유형 매핑 Dict
        - ch_stge_cxe_dict: 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict
        """
        # 0. ReqDto 세팅
        emotion_analysis_dto = EmotionAnlaysisRequestInfo(
            batch_size=req_dto.batch_size,
            batch_sleep=req_dto.batch_sleep,
        )
        
        # 1. 감정 분석 객체 생성
        emotion_analyzer = EmotionAnalyzer(
            llm=self.llm,
            req_info=emotion_analysis_dto,
            emotion_dict=emotion_dict,
            ch_stge_cxe_dict=ch_stge_cxe_dict,
        )

        # 2. 감정 분석 실행
        await emotion_analyzer.execute(voc_list)
        return

    
    async def execute_entity_word_detect(self, req_dto: DataAnalysisReqeustDto, voc_list: List[VocInfo],
                                         ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                                         prev_prdct_svc_word_dict: Dict[str, Set[str]],
                                         prev_pfrm_qalty_word_dict: Dict[str, Set[str]]) -> None:
        """
        [4-2] 개체어 식별
        VOC의 해당하는 개체어을 식별한다.
        =================================================
        @Params
        - voc_list: 필터링 되지 않은 VocList
        - ch_stge_cxe_dict: 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict
        - prev_prdct_svc_word_dict: 채널/고객경험요소 별 이전 실행 상품서비스용어 Set Dict
        - prev_pfrm_qalty_word_dict: 채널/고객경험요소 별 이전 실행 성능품질용어 Set Dict
        """
        # 0. ReqDto 세팅
        entity_word_detect_dto = EntityWordDetectRequestInfo(
            batch_size=req_dto.batch_size,
            voc_chunk_size=req_dto.voc_chunk_size,
            prev_word_input_size=req_dto.prev_word_input_size,
            batch_sleep=req_dto.batch_sleep,
        )
        
        # 1. 개체어 식별 객체 생성
        entity_word_detector = EntityWordDetector(
            llm=self.llm,
            req_info=entity_word_detect_dto,
            ch_stge_cxe_dict=ch_stge_cxe_dict,
            prev_prdct_svc_word_dict=prev_prdct_svc_word_dict,
            prev_pfrm_qalty_word_dict=prev_pfrm_qalty_word_dict,
        )

        # 2. 개체어 식별 실행
        await entity_word_detector.execute(voc_list)
        return

    
    async def execute_voc_problem_reason_detect(self, req_dto: DataAnalysisReqeustDto, voc_list: List[VocInfo],
                                                ch_stge_cxe_dict: Dict[str, Dict[str, Dict[str, Dict[str, str]]]],
                                                ch_stge_dict: Dict[str, List[str]]) -> None:
        """
        [5] VOC 문제원인 식별
        부정 VOC의 문제원인을 식별한다.
        =================================================
        @Params
        - voc_list: 필터링 되지 않고, '부정' 감정대분류인 VocList
        - ch_stge_cxe_dict: 채널/고객경험단계/고객경험요소/고객경험요소 관련 정보 Dict
        - ch_stge_dict: 채널 별 고객경험단계 리스트 Dict 
        """
        # 0. ReqDto 세팅
        voc_problem_reason_detect_dto = VocProblemReasonDetectRequestInfo(
            batch_size=req_dto.batch_size,
            batch_sleep=req_dto.batch_sleep,
        )
        
        # 1. VOC 문제원인 식별 객체 생성
        voc_problem_reason_detector = VocProblemReasonDetector(
            llm = self.llm,
            req_info=voc_problem_reason_detect_dto,
            ch_stge_cxe_dict=ch_stge_cxe_dict,
            ch_stge_dict=ch_stge_dict,
        )
        
        # 2. VOC 문제원인 식별 실행
        await voc_problem_reason_detector.execute(voc_list)
        return

    

    def convert_vo_to_dto_cxe_info(self, agent_id: Optional[str], voc_info: VocInfo) -> ContentResponseDto:
        """
        [6-1] 개별 VOC 결과 별 응답 DTO 세팅
        ===============================
        개별 VOC의 배치 실행 결과를 API 응답 DTO로 변환한다.
        """
        
        # 1. 현재 실행일시 세팅
        today_datetime = datetime.now(pytz.timezone("Asia/Seoul")).strftime('%Y%m%d%H%M%S')

        # 2. 고객경험요소 결과 세팅
        cx_elmnt_clsf_rsult_aray = []
        for cxe_info in voc_info.cxe_result:
            cx_elmnt_clsf_rsult_aray.append(self.convert_vo_to_dto_voc_info(cxe_info))

        # 3. 감정분석 결과 세팅
        cust_expr_cran_emtn_cd = None
        cust_emtn_lag_clsfi_dstcd = None
        cust_expr_vOC_ptrn_dstcd = None
        if voc_info.emtn_result:
            cust_expr_cran_emtn_cd = voc_info.emtn_result.emtn_mid_cd
            cust_emtn_lag_clsfi_dstcd = voc_info.emtn_result.emtn_lag_cd
            cust_expr_vOC_ptrn_dstcd = voc_info.emtn_result.voc_typ_cd

        # 4. 기준 주 결과 세팅
        base_date = datetime.strptime(voc_info.base_ymd, "%Y%m%d")
        base_yw_start = (base_date - timedelta(days=base_date.weekday())).strftime("%Y%m%d")  # 해당 주의 월요일
        base_ym = base_date.strftime('%Y%m')

        # VOC 분석 결과 VO -> DTO로 변환
        return ContentResponseDto(
            #=== VOC 관련 필드 ===#
            group_co_cd=voc_info.group_co_cd,
            base_ymd=voc_info.base_ymd,
            base_yw_start=base_yw_start,
            base_ym=base_ym,
            qusn_iD=voc_info.qusn_id,
            qusn_invl_tagtp_uniq_iD=voc_info.qusn_invl_tagtp_uniq_id,
            qsitm_iD=voc_info.qsitm_id,
            pol_mod_dstcd=voc_info.pol_mod_dstcd,
            pol_knd_dstcd=voc_info.pol_knd_dstcd,
            qsitm_pol_taget_dstcd=voc_info.final_qsitm_pol_taget_dstcd,
            cust_expr_stge_dstcd=voc_info.cx_stge_dstcd,
            qs_sq_dstcd=voc_info.qs_sq_dstcd,
            tran_bnk_dstcd=voc_info.tran_bnk_dstcd,
            qry_inten_lag_dstcd=voc_info.qry_inten_lag_dstcd,
            #=== VOC 필터 결과 ===#
            voc_prcss_ym_yms=today_datetime,
            model_ver=agent_id,
            voc_filtg_yn=voc_info.filtered_yn,
            voc_filtg_resn=voc_info.filtered_reason,
            #=== 고객경험요소매핑 & 개체어 식별 결과 ===#
            cx_elmnt_clsf_sucss_yn=voc_info.cxe_success_yn,
            cx_elmnt_clsf_rsult_aray=cx_elmnt_clsf_rsult_aray,
            #=== 감정분석 결과 ===#
            cust_expr_cran_emtn_cd=cust_expr_cran_emtn_cd,
            cust_emtn_lag_clsfi_dstcd=cust_emtn_lag_clsfi_dstcd,
            cust_expr_vOC_ptrn_dstcd=cust_expr_vOC_ptrn_dstcd,
            #=== 문제원인식별 결과 ===#
            voc_qust_caus_ctnt=voc_info.prblm_reason_result,
            #=== 제공제외여부 결과 ===#
            brnmgr_ofer_yn=voc_info.brnmgr_ofer_yn,
            emp_ofer_yn=voc_info.emp_ofer_yn,
            text_eclud_resn_ctnt=voc_info.text_eclud_resn_ctnt,
        )


    
    def convert_vo_to_dto_voc_info(self, cxe_info: CxeInfo) -> CxeResponseDto:
        """
        [6-1-1] 개별 VOC의 고객경험요소 결과 응답 DTO 세팅
        ===============================
        개별 VOC의 고객경험요소 결과를 API 응답 DTO로 변환한다.
        """
        
        return CxeResponseDto(
            lnp_seq=cxe_info.seq,
            cx_elmnt_cd=cxe_info.cxe_cd,
            cx_elmnt_ctnt=cxe_info.cxe_nm,
            svc_qalty_elmnt_cd=cxe_info.sq_cd,
            prdct_svc_trmn_ctnt=cxe_info.prdct_svc_word,
            prfrm_qalty_trmn_ctnt=cxe_info.pfrm_qalty_word,
        )



        
    #============================================#
    #             TPC 대응용 로직(임시)           #
    #===========================================##
    async def execute_unit(self, user_id: str, messages: list, today_date):
        """
        CHAT 질의 - 기능 유닛별 실행
        """

        # 1. 실행 유닛과 파라미터 세팅
        func_nm, func_params = self.get_execute_parameter(messages)

        # 2. VOC 수집, VOC 객체 세팅
        ch_inst_dict, stge_inst_dict = await self.select_instance_nm()
        req_dto = DataAnalysisReqeustDto(
            agent_id="TEST",
            is_stream=True,
            page=0,
            page_size=0,
            start_ymd="99999999",
            batch_size=10,
        )
        gathered_result = await self.execute_voc_gathering(req_dto)
        voc_info, prev_p_s_word_dict, prev_p_q_word = self.convet_params_to_voc_info(
            user_id=user_id,
            voc_content=func_params.get('VOC원문', ''),
            final_qsitm_pol_taget_dstcd=func_params.get('채널코드', ''),
            cx_stge_dstcd=func_params.get('고객경험단계구분코드', ''),
            rcmdn_resn_qsitm_name=func_params.get('추천이유문항질문', ''),
            rcmdn_resn_qsitm_content=func_params.get('추천이유문항선택항목', ''),
            cxe_cd=func_params.get('고객경험요소코드', ''),
            p_s_word=func_params.get('상품·서비스용어', ''),
            p_q_word=func_params.get('성능·품질용어', ''),
            prev_p_s_word=func_params.get('이전실행 상품·서비스용어목록', ''),
            prev_p_q_word=func_params.get('이전실행 성능·품질용어목록', ''),
            ch_inst_dict=ch_inst_dict,
            stge_inst_dict=stge_inst_dict,
        )
        
        # 3. 실행
        if func_nm == 'VOC필터링': 
            await self.execute_voc_filtering(req_dto, [voc_info], gathered_result[2])
        elif func_nm == "고객경험요소 매핑": 
            await self.execute_cxe_mapping(req_dto, [voc_info], gathered_result[1])
        elif func_nm == "감정 분석": 
            await self.execute_emotion_analysis(req_dto, [voc_info], gathered_result[3], gathered_result[1])
        elif func_nm == "개체어 식별": 
            await self.execute_entity_word_detect(req_dto, [voc_info], gathered_result[1], prev_p_s_word_dict, prev_p_q_word)
        elif func_nm == "문제원인 식별": 
            await self.execute_voc_problem_reason_detect(req_dto, [voc_info], gathered_result[1], gathered_result[2])


        # 4. 응답값 세팅
        return {
            "voc_content": voc_info.orin_voc_content,
            "channel": voc_info.final_qsitm_pol_taget_nm,
            "stage": voc_info.cx_stge_dstic_nm,
            "rcmdn_resn_qsitm_nm": voc_info.rcmdn_resn_qsitm_name,
            "rcmdn_resn_qsitm_content": voc_info.rcmdn_resn_qsitm_content,
            "filtered_yn": voc_info.filtered_yn,
            "filtered_reason": voc_info.filtered_reason,
            "cxe_success_yn": voc_info.cxe_success_yn,
            "cxe_failed_reason": voc_info.cxe_failed_reason,
            "cxe_result": voc_info.cxe_result,
            "emtn_success_yn": voc_info.emtn_success_yn,
            "emtn_failed_reason": voc_info.emtn_failed_reason,
            "emtn_result": voc_info.emtn_result,
            "prblm_reason_success_yn": voc_info.prblm_reason_success_yn,
            "prblm_reason_result": voc_info.prblm_reason_result,
        }
    
    async def select_instance_nm(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:

        result = await self.mcp_executor.execute_tool("mysql_query", {"query": self.instance_sql})
        ch_inst_dict: Dict[str, str] = {}
        stge_inst_dict: Dict[str, str] = {}
        
        for row in result:
            if row['인스턴스식별자'] == '142594000':
                # 고객경험단계구분
                stge_inst_dict[row['인스턴스코드']] = row['인스턴스내용']
            elif row['인스턴스식별자'] == '142447000':
                # 채널
                ch_inst_dict[row['인스턴스코드']] = row['인스턴스내용']

        return ch_inst_dict, stge_inst_dict
        
        
    
    def convet_params_to_voc_info(self, user_id: str,
                                        voc_content: str,
                                        final_qsitm_pol_taget_dstcd: str,
                                        cx_stge_dstcd='00',
                                        rcmdn_resn_qsitm_name=None,
                                        rcmdn_resn_qsitm_content=None,
                                        cxe_cd=None,
                                        p_s_word=None,
                                        p_q_word=None,
                                        prev_p_s_word=[],
                                        prev_p_q_word=[],
                                        ch_inst_dict={},
                                        stge_inst_dict={}) -> Tuple[VocInfo, Dict[str, Set[str]], Dict[str, Set[str]]]:

        test_key = user_id

        prev_word_set_key = DataAnalysisUtils.get_prev_word_set_key(final_qsitm_pol_taget_dstcd, cxe_cd)
        prev_p_s_word_dict = {prev_word_set_key: set(prev_p_s_word)} if prev_p_s_word else {}
        prev_p_q_word_dict = {prev_word_set_key: set(prev_p_q_word)} if prev_p_q_word else {}

        # 채널, 고객경험단계 명
        final_qsitm_pol_taget_nm = ch_inst_dict.get(final_qsitm_pol_taget_dstcd, "")
        final_cx_stge_dstcd = cx_stge_dstcd
        if not cx_stge_dstcd:
            final_cx_stge_dstcd = "00"
        cx_stge_dstic_nm = stge_inst_dict.get(final_cx_stge_dstcd, "")
            
        # 고객경험요소 세팅
        word_success_yn = True if p_s_word or p_q_word else False
        cxe_success_yn = True if cxe_cd else False
        cxe_info = CxeInfo(cxe_cd=cxe_cd,
                           detect_success_yn=word_success_yn,
                           prdct_svc_word=p_s_word,
                           p_q_word=p_q_word)
        
        voc_info = VocInfo(
            # 키
            group_co_cd=test_key,
            base_ymd=test_key,
            qusn_id=test_key,
            qusn_invl_tagtp_uniq_id=test_key,
            qsitm_id=test_key,
            # VOC 관련
            orin_voc_content=voc_content,
            pol_mod_dstcd="TEST",
            final_qsitm_pol_taget_dstcd=final_qsitm_pol_taget_dstcd,
            final_qsitm_pol_taget_nm=final_qsitm_pol_taget_nm,
            cx_stge_dstcd=final_cx_stge_dstcd,
            cx_stge_dstic_nm=cx_stge_dstic_nm,
            rcmdn_resn_qsitm_name=rcmdn_resn_qsitm_name,
            rcmdn_resn_qsitm_content=rcmdn_resn_qsitm_content,
            qry_inten_lag_dstcd="TEST",
            qry_inten_lag_nm="TEST",
            # 고객경험요소 관련
            cxe_success_yn=cxe_success_yn,
            cxe_result=[cxe_info],
        )

        return voc_info, prev_p_s_word_dict, prev_p_q_word_dict
        

    def get_execute_parameter(self, messages) -> Tuple[str, Dict[str, str]]:
        """
        CHAT 질의 선택지 관리
        """
        func_type = {
            "VOC필터링": {
                "format":"채널코드|고객경험단계구분코드(Optional)|VOC원문",
            },
            "고객경험요소 매핑": {
                "format": "채널코드|고객경험단계구분코드(Optional)|VOC원문",
            },
            "감정 분석": {
                "format": "채널코드|고객경험단계구분코드(Optional)|고객경험요소코드(Optional)|추천이유문항질문(Optional)|추천이유문항선택항목(Optional)|VOC원문",
            },
            "개체어 식별": {
                "format": "채널코드|고객경험단계구분코드(Optional)|고객경험요소코드(Optional)|VOC원문|이전실행 상품·서비스용어목록(Optional, ','로구분)|이전실행 성능·품질용어목록(Optional, ','로구분)",
            },
            "문제원인 식별": {
                "format": "채널코드|고객경험단계구분코드(Optional)|고객경험요소코드(Optional)|추천이유문항질문(Optional)|추천이유문항선택항목(Optional)|VOC원문|상품·서비스용어(Optional)|성능·품질용어(Optional)",
            }
        }
        
        exec_func = ""
        exec_param_format = ""
        exec_params = ""
        
        # 1. 실행 단위 설정
        if messages[-5:-3] == [
            ('user', '<데이터분석>'),
            ('assistant', '실행하고 싶은 데이터분석 기능을 선택하세요.'),
        ]:
            selected_func = messages[-3][1]
            input_params = messages[-1][1]
            # 2. 단위 별 파라미터 설정
            for func_nm, params in func_type.items():
                if selected_func == func_nm:
                    exec_func = func_nm
                    exec_param_format = params['format']
                    exec_params = input_params
        
        if not exec_func or not exec_params:
            raise ValueError("실행시킬 기능을 찾지 못했습니다.")
            
        # 3. parameter 검증 및 세팅
        param_dict = self.parse_func_input(exec_params, exec_param_format)

        return exec_func, param_dict

        
    def parse_func_input(self, input_str: str, input_format: str) -> Dict[str, Any]:
        """
        func_name에 해당하는 포맷에 따라 input_str를 파싱하여 딕셔너리로 반환.
        - 구분자는 '|'
        - 필수 필드는 빈값/누락 불가
        - Optional 필드는 빈값/누락 허용 (None으로 반환)
        - "(Optional, ','로구분)" 필드는 값이 있으면 ','로 split하여 리스트로 반환
        검증 실패 시 ValueError 발생
        """
        fields_meta = self._parse_format(input_format)
    
        # 입력 파싱 (빈 값도 유지)
        parts = input_str.split('|')
        # 좌우 공백 제거
        parts = [p.strip() for p in parts]
    
        # parts 개수 검증: 초과 시 에러, 부족 시 Optional만큼은 허용
        if len(parts) > len(fields_meta):
            raise ValueError(f"입력 항목이 너무 많습니다. 기대 개수={len(fields_meta)}, 실제 개수={len(parts)}")
    
        result: Dict[str, Any] = {}
        errors: List[str] = []
    
        for idx, meta in enumerate(fields_meta):
            name = meta["name"]
            optional = meta["optional"]
            list_sep = meta["list_sep"]
    
            value: Optional[str]
            if idx < len(parts):
                value = parts[idx]
            else:
                value = None  # 입력에서 누락
    
            # 검증: 필수 필드 누락/빈값
            if not optional:
                if value is None or value == "":
                    errors.append(f"필수 필드 '{name}' 값이 누락되었거나 비어있습니다.")
                    continue  # 수집은 계속, 마지막에 에러로 처리
    
            # 변환
            if value is None or value == "":
                # Optional인 경우 None으로 설정
                parsed = None
            else:
                if list_sep:
                    # 콤마로 분리하여 리스트로 반환
                    parsed = [v.strip() for v in value.split(',') if v.strip() != ""]
                    # 빈 리스트도 허용(사용자가 콤마만 제공한 경우), 필요 시 None으로 바꾸려면 아래 조건 사용
                    # if len(parsed) == 0:
                    #     parsed = None
                else:
                    parsed = value
    
            result[name] = parsed
    
        if errors:
            raise ValueError("입력 검증 오류: " + "; ".join(errors))
    
        return result
            

    def _parse_format(self, fmt: str) -> List[Dict[str, Any]]:
        """
        포맷 문자열을 파싱해 각 필드의 메타정보를 반환.
        - name: 필드명
        - optional: True/False
        - list_sep: 콤마 분리 여부 (Optional, ','로구분)
        """
        fields_meta = []
        for raw in fmt.split('|'):
            token = raw.strip()
            optional = False
            list_sep = False
    
            # Optional + 콤마 분리 케이스
            if "(Optional, ','로구분)" in token:
                name = token.replace("(Optional, ','로구분)", "").strip()
                optional = True
                list_sep = True
            elif "(Optional)" in token:
                name = token.replace("(Optional)", "").strip()
                optional = True
            else:
                name = token
    
            fields_meta.append({
                "name": name,
                "optional": optional,
                "list_sep": list_sep,
            })
        return fields_meta