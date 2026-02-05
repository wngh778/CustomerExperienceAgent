"""
pytest 실행

```shell
$ cd {개인 compute 경로}/WP-KB0-00107-TA-0001/app/

$ pytest -s tests/data_analysis/test_cxe_mapping.py

# or 특정 함수만 실행
$ pytest -s tests/data_analysis/test_cxe_mapping.py::<function_name>
```
"""
import csv
import time
import pytest

from typing import List, Dict, Any, Optional, Tuple
from pydantic import BaseModel
import pandas as pd

from agent.data_analysis.tools.cxe_mapping import CxeMapper
from agent.data_analysis.tools.voc_filter import VocFilter
from agent.data_analysis.model.vo import VocInfo
from agent.data_analysis.model.dto import CxeMapperRequestInfo, VocFilterRequestInfo



# ----------------------------------------------------------------------
# 테스트용 Vo & Dto 팩터리
# ----------------------------------------------------------------------
def make_req_dto(batch_size: int = 150) -> CxeMapperRequestInfo:
    return CxeMapperRequestInfo(batch_size=batch_size)

def make_voc_info(
    *,
    group_co_cd: str = "G01",
    base_ymd: str = "20240101",
    qusn_id: str = "Q001",
    qusn_invl_tagtp_uniq_id: str = "U001",
    qsitm_id: str = "I001",
    orin_voc_content: str = "",
    final_qsitm_pol_taget_dstcd: str = "02",
    final_qsitm_pol_taget_nm: str = "플랫폼",
    cx_stge_dstcd: str = "18",
    cx_stge_dstic_nm: str = "로그인/인증"
) -> VocInfo:
    """간단히 VocInfo 인스턴스를 만들어 반환."""
    return VocInfo(
        group_co_cd=group_co_cd,
        base_ymd=base_ymd,
        qusn_id=qusn_id,
        qusn_invl_tagtp_uniq_id=qusn_invl_tagtp_uniq_id,
        qsitm_id=qsitm_id,
        # == 요약 필요 데이터 ==
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
    )

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

def print_and_valid_result(voc_info: VocInfo, expect: bool):
    print(f"VOC: {voc_info.get_pk_dict()} 결과:")
    print(f"원문: {voc_info.orin_voc_content}")
    print(f"채널: {voc_info.final_qsitm_pol_taget_nm}, 고객경험단계: {voc_info.cx_stge_dstcd}")
    if not voc_info.cxe_success_yn:
        print(f"분류 실패, 이유: {voc_info.cxe_failed_reason}")
    else:
        print(f"분류 성공")
        
    for cxe_info in voc_info.cxe_result:
        print(f"SEQ: {cxe_info.seq}, 서비스품질요소: {cxe_info.sq_nm}, 고객경험요소: {cxe_info.cxe_nm}")
    print("===================================================")

    assert voc_info.cxe_success_yn == expect

# ----------------------------------------------------------------------
# 1️⃣ voc mock 데이터 테스트 (3~5 건)
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generate_cxe_by_mock():

    # VOC Mock 데이터 세팅
    # 성공 케이스
    voc1 = make_voc_info(qusn_invl_tagtp_uniq_id="1",
                         orin_voc_content="휴대폰 지문등록을  변경하고 스타뱅킹 이용시  로그인 방식을  지문인식으로  변경할때  쉽게 찾을수 없었음. 인증서 관리에서 찾아들어  가야하는데 검색창에는 로그인 방법이나 지문등의 단어로는 검색할수 없었음")
    voc2 = make_voc_info(qusn_invl_tagtp_uniq_id="1",
                         orin_voc_content="중장년층이 편리하게 사용할수 있으면서 보안이 잘 되면 좋을것같다")
    
    # 실패 케이스: 로그인/보안에 앱 VOC
    voc3 = make_voc_info(
        qusn_invl_tagtp_uniq_id="1",
        orin_voc_content="검색이 너무 편했다."
    )
    
    # 성공 케이스: 앱VOC 지만, 단계가 '00'
    voc4 = make_voc_info(
        qusn_invl_tagtp_uniq_id="1",
        orin_voc_content="검색이 너무 편했다.",
        cx_stge_dstcd='00'
    )
    
    voc_list: List[VocInfo] = [voc1, voc2, voc3, voc4]

    ch_sq_cxe_dict, _ = make_cxe_dict()

    # Cxe Mapper 인스턴스 객체 생성
    cxe_mapper = CxeMapper(
        req_info=make_req_dto(),
        ch_sq_cxe_dict=ch_sq_cxe_dict,
    )

    # 실행
    await cxe_mapper.execute(voc_list)

    # 결과 확인
    print_and_valid_result(voc1, True)
    print_and_valid_result(voc2, True)
    print_and_valid_result(voc3, False)
    print_and_valid_result(voc4, True)
        

# ----------------------------------------------------------------------
# 2️⃣ 고객경험요소 ASCII 테이블 테스트
# ----------------------------------------------------------------------
def test_cxe_axcii_table():

    ch_cd = '02'
    ch_sq_cxe_dict, _ = make_cxe_dict()

    # 특정 고객경험단계 not '00'
    # '18' 로그인/인증
    cx_stge_exist: str = CxeMapper.get_cxe_ascii_table(ch_sq_cxe_dict[ch_cd], '18')

    # 특정 고객경험단계 없음 '00'
    cx_stge_not_exist: str = CxeMapper.get_cxe_ascii_table(ch_sq_cxe_dict[ch_cd], '00')

    # '00'은 채널의 모든 고객경험요소를 가지고 있기 때문에, 아닌 경우보다 더 큼
    assert len(cx_stge_exist) < len(cx_stge_not_exist)



# ----------------------------------------------------------------------
# 2️⃣ 잘못된 고객접점단계 테스트
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generate_cxe_wrong_stage():

    # VOC Mock 데이터 세팅
    voc_content = "고객센터 직원이 불친절해요"

    voc_list: List[VocInfo] = [
        make_voc_info(qusn_invl_tagtp_uniq_id='1',
                      orin_voc_content=voc_content)
    ]

    ch_sq_cxe_dict, _ = make_cxe_dict()
    
    # Cxe Mapper 인스턴스 객체 생성
    cxe_mapper = CxeMapper(
        req_info=make_req_dto(),
        ch_sq_cxe_dict=ch_sq_cxe_dict,
    )


    # 실행
    await cxe_mapper.execute(voc_list)

    # 결과 확인
    for voc_info in voc_list:
        print_and_valid_result(voc_info, False)



# ----------------------------------------------------------------------
# 3️⃣ 실제 데이터 대량 테스트 (test할 떄, function 명 앞에 'no_'제거)
# ----------------------------------------------------------------------
class VocWrapping(BaseModel):
    semian_invtg_dstic_nm: Optional[str] # 반기조사구분명
    res_ym: Optional[str]
    voc_info: VocInfo # voc 정보

def setting_voc_info(row: Dict[str, Any]) -> VocWrapping:

    qsitm_pol_taget_dstcd = row['문항설문조사대상구분']
    cust_expr_stge_dstcd = row['고객경험단계구분']
    qsitm_pol_taget_nm=row['문항설문조사대상구분명']

    final_qsitm_pol_taget_dstcd = qsitm_pol_taget_dstcd
    final_qsitm_pol_taget_nm = qsitm_pol_taget_nm
    
    # === VOC 세팅 === #
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
        pol_mod_dstcd=row['설문조사방식구분'],
        pol_mod_dstic_nm=row['설문조사방식구분명'],
        cx_stge_dstcd=cust_expr_stge_dstcd,
        cx_stge_dstic_nm=row['고객경험단계구분명'],
        qsitm_pol_taget_dstcd=qsitm_pol_taget_dstcd,
        qsitm_pol_taget_nm=qsitm_pol_taget_nm,
        
        final_qsitm_pol_taget_dstcd=final_qsitm_pol_taget_dstcd,
        final_qsitm_pol_taget_nm=final_qsitm_pol_taget_nm,
        
        qry_inten_lag_dstcd=row['질문의도대구분'],
        qry_inten_lag_nm=row['질문의도대구분명'],
    )

    
    return VocWrapping(
        semian_invtg_dstic_nm=row.get('반기조사구분명', ""),
        res_ym=row.get('응답년월', ""),
        voc_info=voc_info
    )


PAGE_SIZE = 500
async def select_voc_info_bu(base_year: str, target_channel_code: str) -> List[VocWrapping]:
    
    from core.mcp_util import get_mcp_executor

    executor = await get_mcp_executor()
    
    voc_wrap_list: List[VocWrapping] = []
    offset = 0  # 현재 페이지의 시작 rn
    
    while True:
        # rn 범위를 offset 기반으로 지정
        rn_start = offset + 1
        rn_end = offset + PAGE_SIZE

        query = f"""
        WITH
        voc_res AS (
            SELECT
                  sv73.그룹회사코드
                , sv73.설문응답종료년월일 AS 기준년월일
                , SUBSTR(sv73.설문응답종료년월일, 1, 6) AS 응답년월
                , sv73.설문ID
                , sv73.설문참여대상자고유ID
                , sv73.문항ID
                , sv73.설문조사방식구분
                , 'BU' AS 설문조사방식구분명
                , sv73.설문조사종류구분
                , '고객경험 만족도 조사' AS 설문조사종류구분명
                , CASE
                    WHEN sv73.문항설문조사대상구분 IN ('06', '07')
                    THEN (CASE
                            WHEN sv73.고객경험단계구분 IN ('07','08','09','10','11', '12')
                            THEN '09'
                            ELSE sv73.문항설문조사대상구분
                         END)
                    ELSE sv73.문항설문조사대상구분
                  END AS 문항설문조사대상구분
                , sv73.질문의도대구분
                , sv73.고객경험단계구분
                , sv73.문항응답내용
            FROM inst1.TSCCVSV73 sv73
            WHERE 1=1
              AND sv73.그룹회사코드 = 'KB0'
              AND SUBSTR(sv73.설문응답종료년월일, 1, 6) BETWEEN '{base_year}01' AND '{base_year}10'
              AND sv73.설문조사방식구분 = '02' -- BU
              AND sv73.설문조사종류구분 = '03' -- 고객만족도
              AND sv73.고객경험단계구분 NOT IN ('00', '03') -- 해당없음 / 직원(자가진단용)
              AND sv73.문항구분 IN ('01') -- 서술형
              AND TRIM(sv73.문항응답내용) != '' 
              AND sv73.문항응답내용 IS NOT NULL
              AND CHAR_LENGTH(REPLACE(sv73.문항응답내용, " ", "")) > 10 -- 10음절 밑으로 지우기
            ORDER BY sv73.설문응답종료년월일
        )
        SELECT *
        FROM (
            SELECT 
                  T.그룹회사코드
                , T.기준년월일
                , T.설문ID
                , T.설문참여대상자고유ID
                , T.문항ID
                , T.응답년월
                , T.설문조사방식구분
                , T.설문조사방식구분명
                , T.설문조사종류구분
                , T.설문조사종류구분명
                , T.문항설문조사대상구분
                , polTrg.인스턴스내용 AS 문항설문조사대상구분명
                , T.질문의도대구분
                , qryIntenLag.인스턴스내용 AS 질문의도대구분명
                , T.고객경험단계구분
                , cxStgeDstic.인스턴스내용 AS 고객경험단계구분명
                , T.문항응답내용
                , ROW_NUMBER() OVER (
                    ORDER BY T.그룹회사코드,
                             T.기준년월일,
                             T.설문ID,
                             T.설문참여대상자고유ID,
                             T.문항ID
                ) AS RN
            FROM voc_res T
                LEFT JOIN inst1.TSCCVCI04 qryIntenLag
                     ON qryIntenLag.그룹회사코드 = T.그룹회사코드
                    AND qryIntenLag.인스턴스식별자 = '142457000'
                    AND qryIntenLag.인스턴스코드 = T.질문의도대구분
                LEFT JOIN inst1.TSCCVCI04 polTrg
                     ON polTrg.그룹회사코드 = T.그룹회사코드
                    AND polTrg.인스턴스식별자 = '142447000'
                    AND polTrg.인스턴스코드 = T.문항설문조사대상구분
                LEFT JOIN inst1.TSCCVCI04 cxStgeDstic
                     ON cxStgeDstic.그룹회사코드 = T.그룹회사코드
                    AND cxStgeDstic.인스턴스식별자 = '142594000'
                    AND cxStgeDstic.인스턴스코드 = T.고객경험단계구분
            WHERE 1=1
              AND T.문항설문조사대상구분 = '{target_channel_code}'
        ) A
        WHERE 1=1
          AND A.RN BETWEEN {rn_start} AND {rn_end}
        ORDER BY A.RN
        ;
        """

        print(f"쿼리 (페이지 {offset // PAGE_SIZE + 1}):\n{query}")

        # 설문 문항 조회
        search_result = await executor.execute_tool("mysql_query", {"query": query})
        print(f"페이지 {offset // PAGE_SIZE + 1} 결과 수: {len(search_result)}")

        # 더 이상 결과가 없으면 종료
        if not isinstance(search_result, list) or len(search_result) == 0:
            break

        for row in search_result:
            voc_wrap_list.append(setting_voc_info(row))

        # 다음 페이지로 이동
        offset += PAGE_SIZE

    return voc_wrap_list

    
async def select_voc_info_td(base_year: str, target_channel_code: str) -> List[VocWrapping]:
    
    from core.mcp_util import get_mcp_executor

    executor = await get_mcp_executor()
    
    voc_wrap_list: List[VocWrapping] = []
    offset = 0  # 현재 페이지의 시작 rn
    
    while True:
        # rn 범위를 offset 기반으로 지정
        rn_start = offset + 1
        rn_end = offset + PAGE_SIZE

        query = f"""
        SELECT *
        FROM (
            SELECT
                  sv73.그룹회사코드
                , sv73.설문응답종료년월일 AS 기준년월일
                , sv73.설문ID
                , sv73.설문참여대상자고유ID
                , sv73.문항ID
                , sv73.설문조사방식구분
                , 'TD' AS 설문조사방식구분명
                , sv73.설문조사종류구분
                , 'NPS조사' AS 설문조사종류구분명
                , sv73.문항설문조사대상구분
                , polTrg.인스턴스내용 AS 문항설문조사대상구분명
                , sv73.질문의도대구분
                , qryIntenLag.인스턴스내용 AS 질문의도대구분명
                , sv73.고객경험단계구분
                , cxStgeDstic.인스턴스내용 AS 고객경험단계구분명
                , sv73.문항응답내용
                , sv22.거래은행구분
                , 'KB국민은행' AS 거래은행구분명
                , SUBSTR(sv11.설문응답시작일시, 1, 5) AS 조사년도
                , CASE
                    WHEN DATE_FORMAT(sv11.설문응답시작일시, '%m') <= '06'
                    THEN '1'
                    ELSE '2'
                  END AS 반기조사구분
                , CASE
                    WHEN DATE_FORMAT(sv11.설문응답시작일시, '%m') <= '06'
                    THEN '상반기'
                    ELSE '하반기' END AS 반기조사구분명
                , ROW_NUMBER() OVER (
                    ORDER BY sv73.그룹회사코드,
                             sv73.설문응답종료년월일,
                             sv73.설문ID,
                             sv73.설문참여대상자고유ID,
                             sv73.문항ID
                  ) AS RN
            FROM inst1.TSCCVSV73 sv73
                INNER JOIN inst1.TSCCVSV22 sv22
                     ON sv22.그룹회사코드 = sv73.그룹회사코드
                    AND sv22.설문참여대상자고유ID = sv73.설문참여대상자고유ID
                    AND sv22.거래은행구분 = '1' -- KB국민은행
                INNER JOIN inst1.TSCCVSV11 sv11
                     ON sv11.그룹회사코드 = sv73.그룹회사코드
                    AND sv11.설문ID = sv73.설문ID
                    AND DATE_FORMAT(sv11.설문응답시작일시, '%Y') IN ('{base_year}')
                LEFT JOIN inst1.TSCCVCI04 qryIntenLag
                     ON qryIntenLag.그룹회사코드 = sv73.그룹회사코드
                    AND qryIntenLag.인스턴스식별자 = '142457000'
                    AND qryIntenLag.인스턴스코드 = sv73.질문의도대구분
                LEFT JOIN inst1.TSCCVCI04 polTrg
                     ON polTrg.그룹회사코드 = sv73.그룹회사코드
                    AND polTrg.인스턴스식별자 = '142447000'
                    AND polTrg.인스턴스코드 = sv73.문항설문조사대상구분
                LEFT JOIN inst1.TSCCVCI04 cxStgeDstic
                     ON cxStgeDstic.그룹회사코드 = sv73.그룹회사코드
                    AND cxStgeDstic.인스턴스식별자 = '142594000'
                    AND cxStgeDstic.인스턴스코드 = sv73.고객경험단계구분
            WHERE 1=1
              AND sv73.그룹회사코드 = 'KB0'
              AND sv73.설문조사방식구분 = '01' -- TD
              AND sv73.설문조사종류구분 = '01' -- NPS조사
              AND sv73.문항설문조사대상구분 = '{target_channel_code}' -- 채널
              AND sv73.고객경험단계구분 NOT IN ('00', '03') -- 해당없음 / 직원(자가진단용)
              AND sv73.문항구분 IN ('01') -- 서술형
              AND TRIM(sv73.문항응답내용) != '' 
              AND sv73.문항응답내용 IS NOT NULL
        ) T
        WHERE 1=1
          AND RN BETWEEN {rn_start} AND {rn_end}
        ORDER BY RN
        ;
        """

        print(f"쿼리 (페이지 {offset // PAGE_SIZE + 1}):\n{query}")

        # 설문 문항 조회
        search_result = await executor.execute_tool("mysql_query", {"query": query})
        print(f"페이지 {offset // PAGE_SIZE + 1} 결과 수: {len(search_result)}")

        # 더 이상 결과가 없으면 종료
        if not isinstance(search_result, list) or len(search_result) == 0:
            break

        for row in search_result:
            voc_wrap_list.append(setting_voc_info(row))

        # 다음 페이지로 이동
        offset += PAGE_SIZE

    return voc_wrap_list

    
async def voc_filter(voc_list: List[VocInfo], ch_cx_stage_dict: Dict[str, List[str]]) -> None:
    
    voc_filter = VocFilter(
        req_dto=VocFilterRequestInfo(),
        ch_cx_stage_dict=ch_cx_stage_dict
    )

    await voc_filter.execute(voc_list)


def write_result_csv(voc_wrap_list: List[VocWrapping], base_year: str, target_channel_code: str):
    csv_rows = [[
        '설문조사방식구분', 
        '응답년월', 
        '문항설문조사대상구분', 
        '고객경험단계구분',
        '설문참여대상자고유ID',
        'VOC 원문',
        '서비스품질요소',
        '고객경험요소',
        '분류성공여부',
        '분류실패내용',
    ]]
    # csv_rows = [[
    #     '설문조사방식구분', 
    #     '반기조사구분', 
    #     '거래은행구분',
    #     '문항설문조사대상구분', 
    #     '고객경험단계구분',
    #     '설문참여대상자고유ID',
    #     'VOC 원문',
    #     '서비스품질요소',
    #     '고객경험요소',
    #     '분류성공여부',
    #     '분류실패내용',
    # ]]

    with open(f"{base_year}_bu_nps_{target_channel_code}_cx.csv", 'w', newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        
        for voc_wrap_info in voc_wrap_list:
            voc_info = voc_wrap_info.voc_info
            if voc_info.filtered_yn:
                continue
            
            csv_row = [
                'BU',
                voc_wrap_info.res_ym,
                voc_info.final_qsitm_pol_taget_nm,
                voc_info.cx_stge_dstic_nm,
                voc_info.qusn_invl_tagtp_uniq_id,
                voc_info.orin_voc_content,
                voc_info.sq_nm,
                voc_info.cxe_nm,
                voc_info.cxe_success_yn,
                voc_info.cxe_failed_reason,
            ]
            csv_rows.append(csv_row)

        writer.writerows(csv_rows)




@pytest.mark.asyncio
async def no_test_generate_brief_with_actual_data():
    
    # =====타겟 설정=====
    base_year = '2025'
    target_channel_code = "09"
    # ===================
    
    # 1. 데이터 조회
    voc_wrap_list = await select_voc_info_bu(base_year, target_channel_code)
    voc_list = [voc_wrap.voc_info for voc_wrap in voc_wrap_list]
    ch_sq_cxe_dict, ch_stge_dict = make_cxe_dict()
    
    # 1-2. VOC 필터링
    await voc_filter(voc_list, ch_stge_dict)
    filtered_voc_list = []
    for voc_info in voc_list:
        if not voc_info.filtered_yn:
            filtered_voc_list.append(voc_info)
    
    # 2. CxeMapper 인스턴스 객체 생성
    cxe_mapper = CxeMapper(
        req_info=make_req_dto(),
        ch_sq_cxe_dict=ch_sq_cxe_dict,
    )

    # 3. 실행
    start_time = time.time()
    await cxe_mapper.execute(filtered_voc_list)
    end_time = time.time()
    elapsed_time = start_time - end_time

    print(f"고객경험요소 매핑 종료 건수: {len(filtered_voc_list)}, 소요시간: {elapsed_time}")
    

    # 4. write result csv
    write_result_csv(voc_wrap_list, base_year, target_channel_code)
