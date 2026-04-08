import time, pytz
from datetime import datetime
from pydantic import BaseModel, ConfigDict
from typing import Optional, List

from agent.data_analysis.model.vo import VocInfo, EmotionInfo, CxeInfo

def to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])

class CamelModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True
    )

############################################
##          데이터 분석 요청 용            ##
############################################
class DataAnalysisReqeustDto(BaseModel):
    """
    데이터 분석 에이전트 요청 정보
    """

    # 공통 필드 #
    agent_id: str
    is_stream: bool # 스트리밍 요청 여부. 채팅웹에서의 요청이면 true, NPS 서버 요청이면 false

    
    page: int # 요청하는 페이지 0-Based
    page_size: Optional[int] = 500 # 페이지 사이즈
    
    start_ymd: str # YYYYMMDD
    end_ymd: Optional[str] = None # YYYYMMDD >> None이면, start_date 하루만 분석

    pol_mod_cd_list: Optional[List[str]] = None # 설문조사방식구분코드 리스트 >> None이면 모두
    ch_cd_list: Optional[List[str]] = None # 분석 대상 채널구분코드 리스트 >> None이면 모두

    batch_size: Optional[int] = 150
    batch_sleep: Optional[float] = 1.5
    
    # [VOC 필터] 필드 #
    voc_length_limit: Optional[int] = 10  # 제외할 VOC 원문 음절

    # [개체어 식별] 필드 #
    prev_word_input_size: Optional[int] = 15
    retry_limit: Optional[int] = 3

    # [VOC 필터] & [개체어 식별] 공통 필드 #
    voc_chunk_size: Optional[int] = 50


class CxeResponseDto(CamelModel):
    lnp_seq: int
    cx_elmnt_cd: Optional[str] = None
    cx_elmnt_ctnt: Optional[str] = None
    svc_qalty_elmnt_cd: Optional[str] = None
    prdct_svc_trmn_ctnt: Optional[str] = None
    prfrm_qalty_trmn_ctnt: Optional[str] = None
    

class ContentResponseDto(CamelModel):
    group_co_cd: str
    base_ymd: str
    base_yw_start: str
    base_ym: str
    qusn_iD: str
    qusn_invl_tagtp_uniq_iD: str
    qsitm_iD: str
    pol_mod_dstcd: str
    pol_knd_dstcd: str
    qsitm_pol_taget_dstcd: str
    cust_expr_stge_dstcd: str
    qs_sq_dstcd: Optional[str] = None
    tran_bnk_dstcd: Optional[str] = None
    qry_inten_lag_dstcd: str
    

    # 데이터 분석 처리 결과 #
    voc_prcss_ym_yms: str
    model_ver: Optional[str] = None
    voc_filtg_yn: bool
    voc_filtg_resn: Optional[str] = None

    # 고객경험요소 분류 #
    cx_elmnt_clsf_sucss_yn: bool
    cx_elmnt_clsf_rsult_aray: List[CxeResponseDto] = []
    
    # 감정 분류 결과 #
    cust_expr_cran_emtn_cd: Optional[str] = None
    cust_emtn_lag_clsfi_dstcd: Optional[str] = None
    cust_expr_vOC_ptrn_dstcd: Optional[str] = None

    # 문제원인 식별 결과 #
    voc_qust_caus_ctnt: Optional[str] = None

    # 제공제외여부 결과 #
    brnmgr_ofer_yn: bool = True
    emp_ofer_yn: bool = True
    text_eclud_resn_ctnt: Optional[str] = None

    
class PageResponseDto(CamelModel):
    page_number: int
    page_size: int
    total_elements: int
    total_pages: int
    first_yn: bool
    last_yn: bool
    number_of_elements: int
    content: List[ContentResponseDto] = []

class MetadataResponseDto(CamelModel):
    prcss_mdel_vsnid: str
    prcss_ym_yms: str


class DataAnalysisResponseDto(CamelModel):
    """
    데이터 분석 에이전트 응답 정보
    """
    metadata: MetadataResponseDto
    page: PageResponseDto

    

############################################
##           내부 동작을 용 DTO            ##
############################################
class VocFilterRequestInfo(BaseModel):
    """
    VOC 필터링 요청 정보
    """

    # 제외할 VOC 원문 음절
    voc_length_limit: Optional[int] = 10 
    
    # batch 사이즈
    batch_size: Optional[int] = 150
    # LLM에 한번에 필터링 요청할 VOC의 Chunk 사이즈
    voc_chunk_size: Optional[int] = 50
    batch_sleep: Optional[float] = 1.5
    

class EmotionAnlaysisRequestInfo(BaseModel):
    """
    감정 분석 Agent 요청 정보
    """

    # batch 사이즈
    batch_size: Optional[int] = 150
    batch_sleep: Optional[float] = 1.5


class VocProblemReasonDetectRequestInfo(BaseModel):
    """
    VOC 요약 Agent 요청 정보
    """

    # batch 사이즈
    batch_size: Optional[int] = 150 
    batch_sleep: Optional[float] = 1.5


class CxeMapperRequestInfo(BaseModel):
    """
    고객경험요소 매핑 Agent 요청 정보
    """

    # batch 사이즈
    batch_size: Optional[int] = 150

    # threshold
    cxe_diff_threshold: Optional[int] = 3
    batch_sleep: Optional[float] = 1.5


class EntityWordDetectRequestInfo(BaseModel):
    """
    개체어 식별 Agent 요청 정보
    """

    # batch 사이즈
    batch_size: Optional[int] = 150

    # voc chunk 사이즈 (LLM 요청 한번에 처리할 VOC의 갯수)
    voc_chunk_size: Optional[int] = 50

    # previous word input 사이즈 (Context에 함께 첨부할 이전 실행 결과 용어 갯수 -> 상품서비스용어, 성능품질용어 각각)
    prev_word_input_size: Optional[int] = 20

    # retry limit (함께 첨부된 VOC 중 제외된 결과가 있을 수 있음)
    retry_limit: Optional[int] = 3

    batch_sleep: Optional[float] = 1.5
    