"""
pytest 실행

```shell
$ cd {개인 compute 경로}/WP-KB0-00107-TA-0001/app/

$ pytest -s tests/data_analysis/test_entity_word_detect.py

# or 특정 함수만 실행
$ pytest -s tests/data_analysis/test_entity_word_detect.py::<function_name>
```
"""
import csv
import time
import pytest

import pandas as pd
import pandas

from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel

from agent.data_analysis.tools.entity_word_detect import EntityWordDetector

from agent.data_analysis.model.vo import VocInfo, EmotionInfo, CxeInfo
from agent.data_analysis.model.dto import EntityWordDetectRequestInfo


# ----------------------------------------------------------------------
# 테스트용 Vo & Dto 팩터리
# ----------------------------------------------------------------------
def make_req_dto(batch_size: int = 50, chunk_size: int = 20, prev_size: int = 10) -> EntityWordDetectRequestInfo:
    return EntityWordDetectRequestInfo(
        batch_size=batch_size,
        voc_chunk_size=chunk_size,
        prev_word_input_size=prev_size
    )




def make_cxe_dict() -> Tuple[Dict[str, Dict[str, Dict[str, Dict[str, str]]]], Dict[str, str]]:
    # -------------------------------------------------
    # 1️⃣ CSV 읽어오기
    # -------------------------------------------------
    df = pd.read_excel(
        'tests/data_analysis/sample_files/sq-cxe-ext.xlsx',
        dtype=str,               # 모든 컬럼을 문자열로 읽음 → NaN 방지
    )

    # -------------------------------------------------
    # 2️⃣ “고객경험요소”‑> {sq, desc} 로 변환하는 헬퍼
    # -------------------------------------------------
    def _elem_dict(g: pd.DataFrame) -> Dict[str, Dict[str, str]]:
        """
        g : 한 고객경험단계구분코드에 속한 서브 DataFrame
        반환값 : {
            "고객경험요소": {"sq": 서비스품질요소, "desc": 고객경험요소설명},
            ...
        }
        """
        # 인덱스를 고객경험요소로 잡고, 두 컬럼만 남긴 뒤 이름을 바꾼다.
        return (
            g.set_index('고객경험요소')
             [['서비스품질요소', '고객경험요소설명']]
             .rename(columns={'서비스품질요소': 'sq',
                              '고객경험요소설명': 'desc'})
             .to_dict(orient='index')
        )

    # -------------------------------------------------
    # 3️⃣ 채널 별로 한 번 더 그룹화 → 최종 딕셔너리 구성
    # -------------------------------------------------
    result1: Dict[str, Dict[str, Dict[str, Dict[str, str]]]] = {}
    for channel, df_channel in df.groupby('채널코드'):
        # 각 채널 안에서 고객경험단계구분코드 별로 _elem_dict 적용
        nested_by_code = (
            df_channel.groupby('고객경험단계구분코드')
                      .apply(_elem_dict)
                      .to_dict()
        )
        result1[channel] = nested_by_code

    result2: Dict[str, str] = (
        df.groupby('고객경험단계구분')["고객경험단계구분코드"]
          .first()
          .to_dict()
    )
    
    return result1, result2

def make_entity_word_detector(ch_sq_cxe_dict) -> EntityWordDetector:

    req_dto = make_req_dto()

    return EntityWordDetector(
        req_info=req_dto,
        ch_sq_cxe_dict=ch_sq_cxe_dict,
    )

def make_voc_info(
    *,
    group_co_cd: str = "G01",
    base_ymd: str = "20240101",
    qusn_id: str = "Q001",
    qusn_invl_tagtp_uniq_id: str = "U001",
    qsitm_id: str = "I001",
    orin_voc_content: str = "",
    final_qsitm_pol_taget_dstcd: str = "06",
    final_qsitm_pol_taget_nm: str = "KB 스타뱅킹",
    cx_stge_dstic_nm: str = "로그인/인증",
    cxe_nm: Optional[str] = None,
    rcmdn_resn_qsitm_name: str = "",
    rcmdn_resn_qsitm_content: str = "",
    stge_dict: Dict[str, str] = {},
) -> VocInfo:
    """간단히 VocInfo 인스턴스를 만들어 반환."""

    cxe_info = []
    cxe_success_yn = False
    if cxe_nm:
        cxe_success_yn = True
        cxe_info.append(CxeInfo(cxe_nm=cxe_nm))

    cx_stge_dstcd = stge_dict[cx_stge_dstic_nm]
    
    return VocInfo(
        group_co_cd=group_co_cd,
        base_ymd=base_ymd,
        qusn_id=qusn_id,
        qusn_invl_tagtp_uniq_id=qusn_invl_tagtp_uniq_id,
        qsitm_id=qsitm_id,
        orin_voc_content=orin_voc_content,
        # == 필수값 ==
        cx_stge_dstcd=cx_stge_dstcd,
        cx_stge_dstic_nm=cx_stge_dstic_nm,
        pol_mod_dstcd="",
        pol_mod_dstic_nm="",
        qsitm_pol_taget_dstcd=final_qsitm_pol_taget_dstcd,
        qsitm_pol_taget_nm=final_qsitm_pol_taget_nm,
        final_qsitm_pol_taget_dstcd=final_qsitm_pol_taget_dstcd,
        final_qsitm_pol_taget_nm=final_qsitm_pol_taget_nm,
        qry_inten_lag_dstcd="",
        qry_inten_lag_nm="",
        # == 고객경험요소 ==
        cxe_success_yn=cxe_success_yn,
        cxe_result=cxe_info,
        # == 추천이유문항 ==
        rcmdn_resn_qsitm_name=rcmdn_resn_qsitm_name,
        rcmdn_resn_qsitm_content=rcmdn_resn_qsitm_content,
    )


def print_and_valid_result(voc_info: VocInfo, expect: bool):
    print(f"VOC: {voc_info.get_pk_dict()} 결과:")
    print(f"원문: {voc_info.orin_voc_content}")
    print(f"채널: {voc_info.final_qsitm_pol_taget_nm}, 고객경험단계: {voc_info.cx_stge_dstic_nm}")
    print(f"고객경험요소: {voc_info.cxe_result[0].cxe_nm}")

    rep_cxe = voc_info.cxe_result[0]
    if not rep_cxe.detect_success_yn:
        print(f"개체어 식별 실패")
    else:
        print(f"개체어 식별 성공, 상품서비스용어: {rep_cxe.prdct_svc_word}, 성능품질용어: {rep_cxe.pfrm_qalty_word}")

    # 검증
    assert rep_cxe.detect_success_yn == expect
    print("===================================================")



# ----------------------------------------------------------------------
# 1️⃣ voc mock 데이터 테스트 (3~5 건)
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_entity_word_detect_by_mock():

    ch_sq_cxe_dict, stge_dict = make_cxe_dict()
    
    entity_word_detector = make_entity_word_detector(ch_sq_cxe_dict)

    # VOC Mock 데이터
    voc_list = [
        make_voc_info(
            orin_voc_content="""한번으로 안정적으로
인증등록한점과 신속하게 로그인이
가능한정""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""보안이 뛰어나고 송금이체가 간편한 장점이있다""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""처음 사용하는데 불편하지 않으면서도 보안인증과정들이 복잡하지않고 안정적인 느낌이엇고 출금시 바로 보이스피싱관련위험으로 보호할수있겟구나하는 문자가 와서ㅡ""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""생체인식 로그인이 그다지 철저해보이지 않음""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""주거래 은행이고
이용이 편하고 믿고 사용하고 있어요""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""아직 디지털에 익숙치 못하다보니
잘못됐을경우에 대한 불안감 때문~?""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""다른은행비해~안전하고 친절해서~""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""국민을 생각하는 KB국민은행이니까!
보안에 중심이라고 생각했기때문에""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""인증정보가 노출되지않아서""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""국민은행은 믿음이 가기 때문""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""여러단계를 거치는 과정에서 신뢰가 생겼습니다""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""바쁜 일상속에서 이용은 하고있지만 헝상 조금은 불안한 마음이 있고(보안문제) 원활한 사용방법을 몰라서 힘들어서 중간에 포기한 경우도있슴(상품가입시)""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""별 사고없이 편하게 이용하였기 때문에
앞으로도 은행에서 잘 지켜주리라고 믿는다
그래서 은행에 직접 가서 일보는 지인에게
추천하였다""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""보안이 튼튼하면 신뢰가 올라갑니다.""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""메이저은행이라 믿을만해요.
요즘 보안사고 많이나는데 그런면에서 안심이라 자주 이용하게됩니다.""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""실제로 로그인이 안정적이고, 빠르고, 보안 신뢰가 느껴짐""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""보안이 탄탄해서 좋습니다""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""보안이 확실하다고 생각됨""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""안전하지만 간단한 로그인 방식 덕분에 자주 방문하게 됩니다""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
        make_voc_info(
            orin_voc_content="""현재까지는 보안관리에 아무 문제 없었슴.""",
            cxe_nm="인증 수단의 안전성 체감",
            stge_dict=stge_dict,
        ),
    ]

    # voc_list = [voc1, voc2, voc3, voc4, voc5]

    # 실행
    await entity_word_detector.execute(voc_list)

    # 결과 확인
    for voc in voc_list:
        print_and_valid_result(voc, True)
    # print_and_valid_result(voc2, True)
    # print_and_valid_result(voc3, True)
    # print_and_valid_result(voc4, True)
    # print_and_valid_result(voc5, True)


# ----------------------------------------------------------------------
# 2️⃣ voc 실제 데이터 테스트 (excel 파일)
# ----------------------------------------------------------------------
def setting_voc_info(ch_cd: str, row: pd.Series, stge_dict: Dict[str, str]) -> VocInfo:

    cx_success_yn = ( row['분류성공여부'] == "TRUE" or row['분류성공여부'] == "True" )

    cxe_nm = None
    if cx_success_yn:
        cxe_nm = row['고객경험요소']
    
    return make_voc_info(
        base_ymd=row['응답년월'],
        qusn_invl_tagtp_uniq_id=row['설문참여대상자고유ID'],
        final_qsitm_pol_taget_dstcd=ch_cd,
        final_qsitm_pol_taget_nm=row['문항설문조사대상구분'],
        cx_stge_dstic_nm=row['고객경험단계구분'],
        cxe_nm=cxe_nm,
        orin_voc_content=row['VOC 원문'],
        stge_dict=stge_dict,
    )


BASE_PATH = "tests/data_analysis"

def read_data(target_pol_type: str, base_year: str, ch_cd: str, stge_dict: Dict[str, str]) -> List[VocInfo]:
    # -------------------------------------------------
    # 1️⃣ CSV 읽어오기
    # -------------------------------------------------
    df = pd.read_csv(
        f'{BASE_PATH}/sample_files/{base_year}_{target_pol_type}_nps_{ch_cd}_cx.csv',
        dtype=str,               # 모든 컬럼을 문자열로 읽음 → NaN 방지
    )
    # -------------------------------------------------
    # 2️⃣ VocInfo 세팅
    # -------------------------------------------------
    result: List[VocInfo] = df.apply(lambda row: setting_voc_info(ch_cd, row, stge_dict), axis=1)
    print(len(result))
    # result: List[VocInfo] = [setting_voc_info(ch_cd, row) for row in df.itertuples(index=False)]

    # 메모리 해제
    del df

    return result



def write_result_csv(voc_list: List[VocInfo], target_pol_type: str, base_year: str, ch_cd: str):
    csv_rows = [[
        '응답년월', 
        '문항설문조사대상구분', 
        '고객경험단계구분',
        '설문참여대상자고유ID',
        'VOC 원문',
        '고객경험요소',
        '상품서비스용어',
        '성능품질용어',
        '개체어식별성공여부',
    ]]

    with open(f"{BASE_PATH}/output/{base_year}_{target_pol_type}_nps_{ch_cd}_cx_entity.csv", 'w', newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        
        for voc_info in voc_list:

            cxe_nm = None
            detect_success_yn = False
            prdct_svc_word = None
            pfrm_qalty_word = None
            
            if voc_info.cxe_success_yn:
                cxe_nm = voc_info.cxe_result[0].cxe_nm
                detect_success_yn = voc_info.cxe_result[0].detect_success_yn
                prdct_svc_word = voc_info.cxe_result[0].prdct_svc_word
                pfrm_qalty_word = voc_info.cxe_result[0].pfrm_qalty_word
                
            
            csv_row = [
                voc_info.base_ymd,
                voc_info.final_qsitm_pol_taget_nm,
                voc_info.cx_stge_dstic_nm,
                voc_info.qusn_invl_tagtp_uniq_id,
                voc_info.orin_voc_content,
                cxe_nm,
                prdct_svc_word,
                pfrm_qalty_word,
                detect_success_yn,
            ]
            csv_rows.append(csv_row)

        writer.writerows(csv_rows)

@pytest.mark.asyncio
async def test_entity_word_detect_with_actual_data():

    # =====타겟 설정=====
    target_pol_type = "bu" # td / bu
    base_year = '2025'
    target_channel_code = "09"
    # ===================

    print(f"실행: {target_channel_code}")

    ch_sq_cxe_dict, stge_dict = make_cxe_dict()
    entity_word_detector = make_entity_word_detector(ch_sq_cxe_dict)

    # 1. 데이터 조회
    voc_list: List[VocInfo] = read_data(target_pol_type, base_year, target_channel_code, stge_dict)

    # 2. 필터링 - 실제로 고객경헝요소가 생성된 VOC만 감정분석
    filtered_voc_list = [
        voc_info
        for voc_info in voc_list
        if voc_info.cxe_success_yn
    ]
    
    # 3. 개체어 식별 실행
    start_time = time.time()

    await entity_word_detector.execute(filtered_voc_list)
    
    end_time = time.time()
    print(f"감정분석 종료 건수: {len(filtered_voc_list)}, 소요시간: {start_time - end_time}")
    
    # 4. 결과 Write
    write_result_csv(voc_list, target_pol_type, base_year, target_channel_code)

    
