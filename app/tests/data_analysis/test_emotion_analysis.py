"""
pytest 실행

```shell
$ cd {개인 compute 경로}/WP-KB0-00107-TA-0001/app/

$ pytest -s tests/data_analysis/test_emotion_analysis.py

# or 특정 함수만 실행
$ pytest -s tests/data_analysis/test_emotion_analysis.py::<function_name>
```
"""
import csv
import time
import pytest

import pandas as pd
import pandas

from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel

from agent.data_analysis.tools.emotion_analysis import EmotionAnalyzer

from agent.data_analysis.model.vo import VocInfo, EmotionInfo, CxeInfo
from agent.data_analysis.model.dto import EmotionAnlaysisRequestInfo
from agent.data_analysis.model.consts import EmotionAnalysisMessage


# ----------------------------------------------------------------------
# 테스트용 Vo & Dto 팩터리
# ----------------------------------------------------------------------
def make_req_dto(batch_size: int = 150) -> EmotionAnlaysisRequestInfo:
    return EmotionAnlaysisRequestInfo(batch_size=batch_size)



def make_emotion_dict() -> Dict[str, EmotionInfo]:
    
    # 감정대분류 딕셔너리 세팅
    emotion_large_dict = { 
        "01": "긍정", 
        "02": "부정",
        "03": "중립",
    }
    # voc 유형 딕셔너리 세팅
    voc_type_dict = { 
        '01': '칭찬', 
        '02': '불만', 
        '03': '개선',
        '04': '기타', 
    }

    return {
        '01': EmotionInfo(
            emtn_mid_cd='01',
            emtn_mid_nm='감동',
            emtn_lag_cd='01',
            emtn_lag_nm=emotion_large_dict['01'],
            voc_type_cd='01',
            voc_typ_nm=voc_type_dict['01'],
        ),
        '02': EmotionInfo(
            emtn_mid_cd='02',
            emtn_mid_nm='만족',
            emtn_lag_cd='01',
            emtn_lag_nm=emotion_large_dict['01'],
            voc_typ_cd='01',
            voc_typ_nm=voc_type_dict['01'],
        ),
        '03': EmotionInfo(
            emtn_mid_cd='03',
            emtn_mid_nm='기대',
            emtn_lag_cd='01',
            emtn_lag_nm=emotion_large_dict['01'],
            voc_typ_cd='03',
            voc_typ_nm=voc_type_dict['03'],
        ),
        '04': EmotionInfo(
            emtn_mid_cd='04',
            emtn_mid_nm='아쉬움',
            emtn_lag_cd='02',
            emtn_lag_nm=emotion_large_dict['02'],
            voc_typ_cd='03',
            voc_typ_nm=voc_type_dict['03'],
        ),
        '05': EmotionInfo(
            emtn_mid_cd='05',
            emtn_mid_nm='실망',
            emtn_lag_cd='02',
            emtn_lag_nm=emotion_large_dict['02'],
            voc_typ_cd='02',
            voc_typ_nm=voc_type_dict['02'],
        ),
        '06': EmotionInfo(
            emtn_mid_cd='06',
            emtn_mid_nm='짜증',
            emtn_lag_cd='02',
            emtn_lag_nm=emotion_large_dict['02'],
            voc_typ_cd='02',
            voc_typ_nm=voc_type_dict['02'],
        ),
        '07': EmotionInfo(
            emtn_mid_cd='07',
            emtn_mid_nm='감정 없음',
            emtn_lag_cd='03',
            emtn_lag_nm=emotion_large_dict['03'],
            voc_typ_cd='04',
            voc_typ_nm=voc_type_dict['04'],
        )
    }


def make_cxe_dict() -> Tuple[Dict[str, Dict[str, Dict[str, Dict[str, str]]]], Dict[str, List[str]]]:
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

    result2: Dict[str, List[str]] = (
        df.groupby('채널코드')["고객경험단계구분"]
          .apply(list)
          .to_dict()
    )
    
    return result1, result2

def make_emotion_analyzer() -> EmotionAnalyzer:

    req_dto = make_req_dto()
    emotion_dict = make_emotion_dict()
    ch_sq_cxe_dict, _ = make_cxe_dict()

    return EmotionAnalyzer(
        req_info=req_dto,
        emotion_dict=emotion_dict,
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
    final_qsitm_pol_taget_dstcd: str = "07",
    final_qsitm_pol_taget_nm: str = "영업점",
    cx_stge_dstcd: str = "02",
    cx_stge_dstic_nm: str = "대기",
    cxe_nm: Optional[str] = None,
    rcmdn_resn_qsitm_name: str = "",
    rcmdn_resn_qsitm_content: str = "",
) -> VocInfo:
    """간단히 VocInfo 인스턴스를 만들어 반환."""

    cxe_info = []
    cxe_success_yn = False
    if cxe_nm:
        cxe_success_yn = True
        cxe_info.append(CxeInfo(cxe_nm=cxe_nm))
    
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


def print_and_valid_result(voc_info: VocInfo, expect: str):
    print(f"VOC: {voc_info.get_pk_dict()} 결과:")
    print(f"원문: {voc_info.orin_voc_content}")
    print(f"채널: {voc_info.final_qsitm_pol_taget_nm}, 고객경험단계: {voc_info.cx_stge_dstcd}")
    print(f"문항 제목: {voc_info.rcmdn_resn_qsitm_name}")
    print(f"문항 응답내용: {voc_info.rcmdn_resn_qsitm_content}")
    print(f"고객경험요소: {voc_info.cxe_result[0].cxe_nm}")

    emotion = voc_info.emtn_result
    if not voc_info.emtn_success_yn:
        print(f"감정분석 실패, 이유: {voc_info.emtn_failed_reason}")
        if emotion:
            print(f"환각 감정: {emotion.emtn_mid_nm}")
    else:
        print(f"감정분석 성공, 감정: {emotion.emtn_mid_nm}")

    # 검증
    assert voc_info.emtn_success_yn
    assert emotion.emtn_mid_nm == expect
    print("===================================================")
    


# ----------------------------------------------------------------------
# 1️⃣ voc mock 데이터 테스트 (3~5 건)
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_emotion_analysis_by_mock():

    emotion_analyzer = make_emotion_analyzer()

    # VOC Mock 데이터
    # 대기
    # CASE1. 단순 분석
    voc1 = make_voc_info(
        orin_voc_content="대기시간이 길지 않았다",
        rcmdn_resn_qsitm_name="Q3-1.친구 또는 지인에게 ${부서명}지점의 상담 전 대기경험을 적극적으로 추천하는 이유는 무엇입니까?",
        rcmdn_resn_qsitm_content="고객 대기시간을 줄이기 위한 노력",
        cxe_nm="대기 고객의 집중도 분산",
    )
    # CASE2. 추천이유 응답 내용 보다 VOC에서 추출
    voc2 = make_voc_info(
        orin_voc_content="대기 시간이 길어요",
        rcmdn_resn_qsitm_name="Q3-1.친구 또는 지인에게 ${부서명}지점의 상담 전 대기경험을 적극적으로 추천하는 이유는 무엇입니까?",
        rcmdn_resn_qsitm_content='확인하기 쉬운 대기정보(대기순번, 대기예상시간)',
        cxe_nm='대기 고객의 집중도 분산',
    )
    # CASE3. 격한 반응 -> 감동
    voc3 = make_voc_info(
        cx_stge_dstcd='01', # 내점/방문
        orin_voc_content="ATM기 이용이 정말정말 편해요!!!",
        rcmdn_resn_qsitm_name="Q2-1.친구 또는 지인에게 ${부서명}지점의 영업점 내점/방문과정 경험을 적극적으로 추천하는 이유는 무엇입니까?",
        rcmdn_resn_qsitm_content='이용하기 쉬운 자동화기기(ATM, STM 등)',
        cxe_nm='ATM 이용 안내의 명확성',
    )
    
    # CASE4,5. 같은 VOC여도 고객경험요소에 따라 분석 대상 문장 선정
    voc4 = make_voc_info(
        cx_stge_dstcd='00', # 해당무
        orin_voc_content="상담을통한 적절한상품과다양한상품에대한충분한설명을 듣는다\n객장은 속도가느리고 고객의입장에서답답하다. 좀더빠른업무기대한다\n상담실을통한 은행일을본다 객장에서볼일은 가급적하지않으려고한다",
        rcmdn_resn_qsitm_name="",
        rcmdn_resn_qsitm_content='',
        cxe_nm='다양한 상품 비교 및 제시',
    )
    voc5 = make_voc_info(
        cx_stge_dstcd='00', # 해당무
        orin_voc_content="상담을통한 적절한상품과다양한상품에대한충분한설명을 듣는다\n객장은 속도가느리고 고객의입장에서답답하다. 좀더빠른업무기대한다\n상담실을통한 은행일을본다 객장에서볼일은 가급적하지않으려고한다",
        rcmdn_resn_qsitm_name="",
        rcmdn_resn_qsitm_content='',
        cxe_nm='신속한 상담 및 업무 처리',
    )

    voc_list = [voc1, voc2, voc3, voc4, voc5]

    # 실행
    await emotion_analyzer.execute(voc_list)

    # 결과 확인
    print_and_valid_result(voc1, "만족")
    print_and_valid_result(voc2, "아쉬움")
    print_and_valid_result(voc3, "감동")
    print_and_valid_result(voc4, "만족")
    print_and_valid_result(voc5, "아쉬움")


# ----------------------------------------------------------------------
# 2️⃣ voc 실제 데이터 테스트 (excel 파일)
# ----------------------------------------------------------------------
def setting_voc_info(ch_cd: str, row: pd.Series) -> VocInfo:

    cx_success_yn = ( row['분류성공여부'] == "True" )

    cxe_nm = None
    if cx_success_yn:
        cxe_nm = row['고객경험요소']
    
    return make_voc_info(
        base_ymd=row['응답년월'],
        qusn_invl_tagtp_uniq_id=row['설문참여대상자고유ID'],
        final_qsitm_pol_taget_dstcd=ch_cd,
        final_qsitm_pol_taget_nm=row['문항설문조사대상구분'],
        cx_stge_dstcd='00',
        cx_stge_dstic_nm=row['고객경험단계구분'],
        cxe_nm=cxe_nm,
        orin_voc_content=row['VOC 원문'],
    )


BASE_PATH = "tests/data_analysis"

def read_data(target_pol_type: str, base_year: str, ch_cd: str) -> List[VocInfo]:
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
    result: List[VocInfo] = df.apply(lambda row: setting_voc_info(ch_cd, row), axis=1)
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
        '감정중분류',
        '감정대분류',
        'VOC유형',
        '감정분석성공여부',
        '감정분석실패내용',
    ]]

    with open(f"{BASE_PATH}/output/{base_year}_{target_pol_type}_nps_{ch_cd}_cx_emotion.csv", 'w', newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        
        for voc_info in voc_list:

            emtn_mid_nm = None
            emtn_lag_nm = None
            voc_typ_nm = None
            
            emtn_result = voc_info.emtn_result
            if emtn_result:
                emtn_mid_nm = emtn_result.emtn_mid_nm
                emtn_lag_nm = emtn_result.emtn_lag_nm
                voc_typ_nm = emtn_result.voc_typ_nm

            cxe_nm = None
            if voc_info.cxe_success_yn:
                cxe_nm = voc_info.cxe_result[0].cxe_nm
            
            csv_row = [
                voc_info.base_ymd,
                voc_info.final_qsitm_pol_taget_nm,
                voc_info.cx_stge_dstic_nm,
                voc_info.qusn_invl_tagtp_uniq_id,
                voc_info.orin_voc_content,
                cxe_nm,
                emtn_mid_nm,
                emtn_lag_nm,
                voc_typ_nm,
                voc_info.emtn_success_yn,
                voc_info.emtn_failed_reason,
            ]
            csv_rows.append(csv_row)

        writer.writerows(csv_rows)

@pytest.mark.asyncio
async def test_emotion_analysis_with_actual_data():

    # =====타겟 설정=====
    target_pol_type = "bu" # td / bu
    base_year = '2025'
    target_channel_code = "09"
    # ===================

    print(f"실행: {target_channel_code}")


    # 1. 데이터 조회
    voc_list: List[VocInfo] = read_data(target_pol_type, base_year, target_channel_code)

    # 2. 필터링 - 실제로 고객경헝요소가 생성된 VOC만 감정분석
    filtered_voc_list = [
        voc_info
        for voc_info in voc_list
        if voc_info.cxe_success_yn
    ]
    
    # 3. 감정분석 실행
    emotion_analyzer = make_emotion_analyzer()
    start_time = time.time()

    await emotion_analyzer.execute(filtered_voc_list)
    
    end_time = time.time()
    print(f"감정분석 종료 건수: {len(filtered_voc_list)}, 소요시간: {start_time - end_time}")
    
    # 4. 결과 Write
    write_result_csv(voc_list, target_pol_type, base_year, target_channel_code)
    
    