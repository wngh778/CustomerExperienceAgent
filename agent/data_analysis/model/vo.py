from pydantic import BaseModel
from typing import Optional, Dict, Any, List

class EmotionInfo(BaseModel):
    """
    감정 정보
    """
    
    emtn_mid_cd: Optional[str] = None # 감정중분류 코드
    emtn_mid_nm: str # 감정중분류 이름

    emtn_lag_cd: Optional[str] = None # 감정대분류 코드
    emtn_lag_nm: Optional[str] = None # 감정대분류 명

    voc_typ_cd: Optional[str] = None # VOC유형 코드
    voc_typ_nm: Optional[str] = None # VOC유형 명
    

class CxeInfo(BaseModel):
    
    seq: Optional[int] = 1
    
    # 서비스품질요소
    sq_cd: Optional[str] = None # 서비스품질요소코드
    sq_nm: Optional[str] = None # 서비스품질요소명

    # 고객경험요소
    cxe_cd: Optional[str] = None # 고객경험요소 ID
    cxe_nm: Optional[str] = None # 고객경험요소명

    # 개체어
    detect_success_yn: bool = False
    prdct_svc_word: Optional[str] = None
    pfrm_qalty_word: Optional[str] = None
    


class VocInfo(BaseModel):
    """
    설문 VOC 정보
    """
    
    #================VOC PK=================#
    group_co_cd: str # 그룹회사 코드
    base_ymd: str # 기준년월일
    qusn_id: str # 설문ID
    qusn_invl_tagtp_uniq_id: str # 설문참여대상자고유ID
    qsitm_id: str # 문항ID

    #=========VOC 관련 정보 필드============#
    orin_voc_content: str # VOC 원문

    tran_bnk_dstcd: Optional[str] = None # 거래은행구분코드
    tran_bnk_dstic_nm: Optional[str] = None # 거래은행구분명

    pol_knd_dstcd: Optional[str] = None # 설문조사종류구분코드
    
    pol_mod_dstcd: str # 설문조사방식구분코드
    pol_mod_dstic_nm: Optional[str] = None # 설문조사방식구분명

    final_qsitm_pol_taget_dstcd: str # 실제채널코드 (영업점/스타뱅킹일 떄, 단계에 따라 '09'로 정정)
    final_qsitm_pol_taget_nm: str # 실제채널명 (영업점/스타뱅킹일 떄, 단계에 따라 '상품'로 정정)

    cx_stge_dstcd: str # 고객경험단계구분코드
    cx_stge_dstic_nm: str # 고객경험단계구분명
    
    qry_inten_lag_dstcd: str # 질문의도대구분
    qry_inten_lag_nm: str # 질문의도대구분명
    
    rcmdn_rson_qsitm_id: Optional[str] = None      # 추천이유문항ID
    rcmdn_resn_qsitm_name: Optional[str] = None    # 추천이유문항제목
    rcmdn_resn_qsitm_desc: Optional[str] = None    # 추천이유문항설명
    rcmdn_resn_qsitm_content: Optional[str] = None # 추천이유문항응답내용
    qs_sq_dstcd: Optional[str] = None # 문항서비스품질요소

    #=============수행 결과 필드==============#
    # [VOC 필터링 결과]
    filtered_yn: bool = False
    filtered_reason: Optional[str] = None
    
    # [고객경험요소 매칭 결과]
    cxe_success_yn: bool = False # 분류 성공 여부
    cxe_failed_reason: Optional[str] = None # 분류 실패 시, 이유
    cxe_result: List[CxeInfo] = [ CxeInfo() ] # 분류 결과 리스트 

    # [감정분석 결과]
    emtn_success_yn: bool = False # 감정 분석 성공 여부
    emtn_failed_reason: Optional[str] = None # 실패 이유
    emtn_result: Optional[EmotionInfo] = None # 감정분석 결과

    # [VOC 문제원인식별 결과]
    prblm_reason_success_yn: bool = False # voc 문제원인 식별 성공여부
    prblm_reason_result: Optional[str] = None # voc 문제원인 결과

    # [제공제외여부 결과]
    brnmgr_ofer_yn: bool = True
    emp_ofer_yn: bool = True
    text_eclud_resn_ctnt: Optional[str] = None

    
    def get_pk_dict(self) -> Dict[str, Any]:
        """
        VOC 데이터의 PK 값들을 ArrayList로 반환
        """
        
        # [
        #     그룹회사코드, 설문응답종료년월일, 설문ID, 
        #     설문참여대상자고유ID, 문항ID
        # ]
        return {
            'group_co_cd': self.group_co_cd, 
            'base_ymd': self.base_ymd, 
            'qusn_id': self.qusn_id,
            'qusn_invl_tagtp_uniq_id': self.qusn_invl_tagtp_uniq_id, 
            'qsitm_id': self.qsitm_id
        }