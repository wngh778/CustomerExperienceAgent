"""
pytest 실행

```shell
$ cd {개인 compute 경로}/WP-KB0-00107-TA-0001/app/

$ pytest -s tests/data_analysis/test_voc_brief.py

# or 특정 함수만 실행
$ pytest -s tests/data_analysis/test_voc_brief.py::<function_name>
```
"""
import csv
import time
import pytest

from typing import List, Dict, Any
from pydantic import BaseModel

from agent.data_analysis.tools.voc_brief import VocSummerizer
from agent.data_analysis.tools.voc_filter import VocFilter
from agent.data_analysis.model.vo import VocInfo
from agent.data_analysis.model.dto import VocBriefRequestInfo



# ----------------------------------------------------------------------
# 테스트용 Vo & Dto 팩터리
# ----------------------------------------------------------------------
def make_req_dto(batch_size: int = 150) -> VocBriefRequestInfo:
    return VocBriefRequestInfo(batch_size=batch_size)
    
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

def make_cx_stage_dict() -> Dict[str, List[str]]:
    return {
        '01': ['고객중심','전문성','혁신주도(미래가치)','신뢰정직','동반성장(ESG)','고객소통'],
        '02': ['로그인/인증','홈화면','계좌조회/이체','통합검색','금융상품몰','상품가입','상품관리/해지','콘텐츠/서비스'],
        '03': ['내점/방문','대기','맞이/의도파악','직원상담','업무처리/배웅'],
        '04': ['직원상담','버튼식ARS','보이는ARS','대기','챗봇상담','콜봇상담'],
        '05': ['저축성','여신성','투자성','보장성','외화환전','해외송금'],
    }


# ----------------------------------------------------------------------
# 1️⃣ voc mock 데이터 테스트 (3~5 건)
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generate_brief_by_mock():

    # VOC Mock 데이터 세팅
    voc_list: List[VocInfo] = [
        make_voc_info(qusn_invl_tagtp_uniq_id="1",
                      orin_voc_content="휴대폰 지문등록을  변경하고 스타뱅킹 이용시  로그인 방식을  지문인식으로  변경할때  쉽게 찾을수 없었음. 인증서 관리에서 찾아들어  가야하는데 검색창에는 로그인 방법이나 지문등의 단어로는 검색할수 없었음"),
        make_voc_info(qusn_invl_tagtp_uniq_id="1",
                      orin_voc_content="중장년층이 편리하게 사용할수 있으면서 보안이 잘 되면 좋을것같다"),
        make_voc_info(qusn_invl_tagtp_uniq_id="1",
                      orin_voc_content="로그인 화면은\n직관적이고 좋음\n다만 앱이 전반적으로 산만함"),
        make_voc_info(qusn_invl_tagtp_uniq_id="1",
                      orin_voc_content="보안때문에 인증이 강화좋은데 복잡한 점도 있습니다"),
    ]

    # VOC Summerizer 인스턴스 객체 생성
    voc_summerizer = VocSummerizer(
        req_info=make_req_dto(),
        cx_stage_nm_dict=make_cx_stage_dict()
    )

    # 실행
    await voc_summerizer.execute(voc_list)

    # 결과 확인
    for voc_info in voc_list:
        print(f"VOC: {voc_info.get_pk_dict()} 생성된 요약:")
        print(f"{voc_info.voc_brief_content}")
        print("===================================================")

        assert len(voc_info.voc_brief_content) != 0
    
# ----------------------------------------------------------------------
# 2️⃣ 고객접점단계 유무 테스트
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generate_brief_stage_yn():

    # VOC Mock 데이터 세팅
    voc_content = "로그인 화면은\n직관적이고 좋음\n다만 앱 UI/UX가 전반적으로 산만함"

    voc_list: List[VocInfo] = [
        # 고객접점단계 있음 (로그인/인증에 관한 내용만 나와야함)
        make_voc_info(qusn_invl_tagtp_uniq_id='1',
                      orin_voc_content=voc_content),
        # 고객접점단계 없음 (채널의 모든 단계에 대해 나와도 됨)
        make_voc_info(qusn_invl_tagtp_uniq_id='2',
                      orin_voc_content=voc_content,
                      cx_stge_dstcd="")
    ]

    # VOC Summerizer 인스턴스 객체 생성
    voc_summerizer = VocSummerizer(
        req_info=make_req_dto(),
        cx_stage_nm_dict=make_cx_stage_dict()
    )

    # 실행
    await voc_summerizer.execute(voc_list)

    # 결과 확인
    for voc_info in voc_list:
        print(f"VOC: {voc_info.get_pk_dict()} 생성된 요약:")
        print(f"{voc_info.voc_brief_content}")
        print("===================================================")

    # 두 값 비교
    assert voc_list[0].voc_brief_content != voc_list[1].voc_brief_content



# ----------------------------------------------------------------------
# 3️⃣ 실제 데이터 대량 테스트 (test할 떄, function 명 앞에 'no_'제거)
# ----------------------------------------------------------------------
class VocWrapping(BaseModel):
    semian_invtg_dstic_nm: str # 반기조사구분명
    voc_info: VocInfo # voc 정보

def setting_voc_info(row: Dict[str, Any]) -> VocWrapping:

    qsitm_pol_taget_dstcd = row['문항설문조사대상구분']
    cust_expr_stge_dstcd = row['고객경험단계구분']
    qsitm_pol_taget_nm=row['문항설문조사대상구분명']

    final_qsitm_pol_taget_dstcd = qsitm_pol_taget_dstcd
    final_qsitm_pol_taget_nm = qsitm_pol_taget_nm
    if qsitm_pol_taget_dstcd == '07' or qsitm_pol_taget_dstcd == '06':
        if cust_expr_stge_dstcd == '07' or cust_expr_stge_dstcd == '08' or cust_expr_stge_dstcd == '09' or cust_expr_stge_dstcd == '10' or cust_expr_stge_dstcd == '11' or cust_expr_stge_dstcd == '12':
            final_qsitm_pol_taget_dstcd = '09'
            final_qsitm_pol_taget_nm = '상품'
    
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
        semian_invtg_dstic_nm=row['반기조사구분명'],
        voc_info=voc_info
    )


PAGE_SIZE = 500
async def select_voc_info(base_year: str, target_channel_code: str) -> List[VocWrapping]:
    
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
              -- AND sv73.고객경험단계구분 = '18' -- 로그인/인증
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

async def voc_filter(voc_list: List[VocInfo]) -> None:
    voc_filter = VocFilter()

    await voc_filter.execute(voc_list)


def write_result_csv(voc_wrap_list: List[VocWrapping], base_year: str, target_channel_code: str):
    csv_rows = [[
        '설문조사방식구분', 
        '반기조사구분', 
        '거래은행구분',
        '문항설문조사대상구분', 
        '고객경험단계구분',
        '설문참여대상자고유ID',
        'VOC 원문', 
        'VOC 요약'
    ]]

    with open(f"{base_year}_td_nps_{target_channel_code}.csv", 'w', newline="", encoding="utf-8-sig") as f:

        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        
        for voc_wrap_info in voc_wrap_list:
            voc_info = voc_wrap_info.voc_info
            if voc_info.filtered_yn:
                continue
            
            csv_row = [
                'TD',
                voc_wrap_info.semian_invtg_dstic_nm,
                'KB국민은행',
                voc_info.final_qsitm_pol_taget_nm,
                voc_info.cx_stge_dstic_nm,
                voc_info.qusn_invl_tagtp_uniq_id,
                voc_info.orin_voc_content,
                voc_info.voc_brief_content
            ]
            csv_rows.append(csv_row)

        writer.writerows(csv_rows)
            



@pytest.mark.asyncio
async def test_generate_brief_with_actual_data():
    
    # =====타겟 설정=====
    base_year = '2025'
    target_channel_code = "05"
    # ===================
    
    # 1. 데이터 조회
    voc_wrap_list = await select_voc_info(base_year, target_channel_code)
    voc_list = [voc_wrap.voc_info for voc_wrap in voc_wrap_list]

    # 1-2. VOC 필터링
    await voc_filter(voc_list)
    filtered_voc_list = []
    for voc_info in voc_list:
        if not voc_info.filtered_yn:
            filtered_voc_list.append(voc_info)
    
    # 2. VOC Summerizer 인스턴스 객체 생성
    voc_summerizer = VocSummerizer(
        req_info=make_req_dto(),
        cx_stage_nm_dict=make_cx_stage_dict()
    )

    # 3. 실행
    start_time = time.time()
    await voc_summerizer.execute(filtered_voc_list)
    end_time = time.time()
    elapsed_time = start_time - end_time

    print(f"VOC 요약 종료 건수: {len(filtered_voc_list)}, 소요시간: {elapsed_time}")
    

    # 4. write result csv
    write_result_csv(voc_wrap_list, base_year, target_channel_code)