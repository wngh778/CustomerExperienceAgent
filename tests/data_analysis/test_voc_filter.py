"""
pytest 실행

```shell
$ pytest -s tests/data_analysis/test_voc_filter.py

# or 특정 함수만 실행
$ pytest -s tests/data_analysis/test_voc_filter.py::<function_name>
```
"""

import asyncio
import itertools
import logging
from typing import List

import pytest

from langchain.schema import HumanMessage, SystemMessage

from agent.data_analysis.tools.voc_filter import VocFilter 
from agent.data_analysis.model.vo import VocInfo
from agent.data_analysis.model.dto import VocFilterRequestInfo
from agent.data_analysis.model.consts import VocFilterMessage

# 상수 import (내부적으로 사용되는)
from agent.data_analysis.tools.voc_filter.voc_filter import STAGE_EXIST, STAGE_NOT_EXIST




# ----------------------------------------------------------------------
# 테스트용 VocInfo 팩터리
# ----------------------------------------------------------------------
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
    
def make_voc_filter(lenth_limit: int = 5,
                    chunk_size: int = 3) -> VocFilter:
    
    req_dto = VocFilterRequestInfo(
        voc_length_limit=lenth_limit,
        voc_chunk_size=chunk_size,
    )

    ch_cx_stage_dict = {
        "02": ["로그인/인증","홈화면","통합검색","금융상품몰","상품가입","계좌조회/이체","상품관리/해지","콘텐츠/서비스",]
    }
        
    
    return VocFilter(
        req_dto=req_dto,
        ch_cx_stage_dict=ch_cx_stage_dict
    )



# ----------------------------------------------------------------------
# 1️⃣ distinct_voc_list 테스트
# ----------------------------------------------------------------------
def test_distinct_voc_list_marks_duplicates():
    """PK 가 같은 객체는 filtered_yn=True 로 표시되는지 확인."""
    # 동일 PK 를 가진 3개의 객체 생성
    v1 = make_voc_info(orin_voc_content="첫번째")
    v2 = make_voc_info(orin_voc_content="두번째")   # PK 동일
    v3 = make_voc_info(orin_voc_content="세번째")   # PK 동일

    vocs: List[VocInfo] = [v1, v2, v3]

    # distinct_voc_list 는 static 메서드가 아니므로 인스턴스 생성 후 호출
    vf = make_voc_filter()
    vf.distinct_voc_list(vocs)

    # 첫 번째는 그대로, 나머지는 filtered_yn=True
    assert not v1.filtered_yn
    assert v2.filtered_yn
    assert v3.filtered_yn



# ----------------------------------------------------------------------
# 2️⃣ filter_voc_content 테스트 (패턴, 길이, 반복 문자 등)
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "content, expected_filtered, reason",
    [
        # ① 한글 자모만 남은 경우 → 길이 0 → 필터링
        ("ㄱㄴㄷ", True, VocFilterMessage.TEXT_FILTER),
        # ② 숫자·특수문자만 남은 경우 → 길이 0 → 필터링
        ("123!@#", True, VocFilterMessage.TEXT_FILTER),
        # ③ 연속 반복 문자 → collapse 후 길이 1 (필터링)
        ("aaaaa", True, VocFilterMessage.TEXT_FILTER),
        # ④ 정상적인 문자열 (길이 6 > limit 5) → 통과
        ("안녕하세요 여러분", False, None),
        # ⑤ 길이 제한 이하 (limit=5) → 필터링
        ("안녕", True, VocFilterMessage.TEXT_FILTER),
    ],
)
def test_filter_voc_content(content, expected_filtered, reason):
    voc = make_voc_info(orin_voc_content=content)

    voc_filter = make_voc_filter()
    voc_filter.filter_voc_content(voc)

    assert voc.filtered_yn == expected_filtered
    if expected_filtered:
        assert voc.filtered_reason == reason.value
    else:
        assert not voc.filtered_reason


# ----------------------------------------------------------------------
# 3️⃣ _collapse_repeats, _length_check 정적 메서드 테스트
# ----------------------------------------------------------------------
def test_collapse_and_length_check():
    # collapse
    assert VocFilter._collapse_repeats("aaabbbcc") == "abc"
    assert VocFilter._collapse_repeats("ㅋㅋㅋㅋ") == "ㅋ"

    # length_check (limit = 5)
    assert VocFilter._length_check("hello", 5) is False   # == limit → False
    assert VocFilter._length_check("helloo", 5) is True   # > limit → True
    assert VocFilter._length_check("", 5) is False        # empty → False


# ----------------------------------------------------------------------
# 4️⃣ convert 메서드 테스트 (중첩 딕셔너리 생성)
# ----------------------------------------------------------------------
def test_convert_groups_by_stage_and_channel():
    # filtered_yn=False 인 것만 포함돼야 함
    v1 = make_voc_info(orin_voc_content="A", final_qsitm_pol_taget_dstcd="99", cx_stge_dstcd="01")
    v2 = make_voc_info(orin_voc_content="B", final_qsitm_pol_taget_dstcd="99", cx_stge_dstcd="00")  # stage 없음
    v3 = make_voc_info(orin_voc_content="C", final_qsitm_pol_taget_dstcd="01", cx_stge_dstcd="01")
    v4 = make_voc_info(orin_voc_content="D", final_qsitm_pol_taget_dstcd="01", cx_stge_dstcd="00")
    v4.filtered_yn = True   # 필터링된 건 제외

    result = VocFilter.convert([v1, v2, v3, v4])

    # outer key : final_qsitm_pol_taget_dstcd (채널)
    # inner key : STAGE_EXIST / STAGE_NOT_EXIST
    assert set(result.keys()) == {"99", "01"}

    # 채널 99
    assert len(result["99"][STAGE_EXIST]) == 1         # stage 존재
    assert len(result["99"][STAGE_NOT_EXIST]) == 1     # stage 없음

    # 채널 01
    assert result["01"][STAGE_EXIST][0].orin_voc_content == "C"
    # 필터링된 v4 은 결과에 포함되지 않음
    assert STAGE_NOT_EXIST not in result["01"]


# ----------------------------------------------------------------------
# 5️⃣ get_prompt 라우팅 테스트 (exist_stage True/False)
# ----------------------------------------------------------------------
def test_get_prompt_routing():
    voc_filter = make_voc_filter()
    vocs = [make_voc_info(orin_voc_content="test")]
    
    # ① exist_stage = True → _get_prompt_stge 호출
    msgs = voc_filter.get_prompt(ch_cd="02", voc_infos=vocs, exist_stage=True)
    # 첫 번째 메시지는 SystemMessage, 두 번째는 HumanMessage
    assert isinstance(msgs[0], SystemMessage)
    assert isinstance(msgs[1], HumanMessage)
    assert "| **`VOC 고객경험단계구분`** |" in msgs[0].content
    assert "| VOC 고객경험단계구분 |" in msgs[1].content
    
    # ② exist_stage = False → _get_prompt_ch 호출
    msgs2 = voc_filter.get_prompt(ch_cd="02", voc_infos=vocs, exist_stage=False)
    assert isinstance(msgs2[0], SystemMessage)
    assert isinstance(msgs2[1], HumanMessage)
    assert "| **`VOC 고객경험단계구분`** |" not in msgs2[0].content
    assert "| VOC 고객경험단계구분 |" not in msgs2[1].content

# ----------------------------------------------------------------------
# 6️⃣ create_nlp_tasks 의 chunk, stage 구분 로직 검증
# ----------------------------------------------------------------------
def test_create_nlp_tasks_chunking():

    voc_filter = make_voc_filter()
    
    # 채널 99 에 대해 stage 존재/미존재 각각 2개의 VocInfo 를 넣는다.
    v1 = make_voc_info(qsitm_id="1", orin_voc_content="계좌이체가 잘 안됨", final_qsitm_pol_taget_dstcd="02", cx_stge_dstcd="18", cx_stge_dstic_nm="로그인/인증")
    v2 = make_voc_info(qsitm_id="2", orin_voc_content="지문인식이 살짝 느린것 같음", final_qsitm_pol_taget_dstcd="02", cx_stge_dstcd="18", cx_stge_dstic_nm="로그인/인증")
    v3 = make_voc_info(qsitm_id="3", orin_voc_content="직원 표정이 그냥 마음에 안듦", final_qsitm_pol_taget_dstcd="02", cx_stge_dstcd="00")
    v4 = make_voc_info(qsitm_id="4", orin_voc_content="검색이 잘 됐어요.", final_qsitm_pol_taget_dstcd="02", cx_stge_dstcd="00")
    
    # convert 로 만든 중첩 dict
    grouped = VocFilter.convert([v1, v2, v3, v4])

    tasks = voc_filter.create_nlp_tasks(ch_cd="02", ch_voc_dict=grouped["02"])

    # voc_chunk_size 가 3 이므로 각 stage 별로 2개씩 들어가면 1개의 chunk 로 묶인다.
    # 따라서 전체 tasks 개수는 2 (stage_exist, stage_not_exist) 이어야 함.
    assert len(tasks) == 2

    # 각 task 는 coroutine 이므로 실행해 본다.
    # 첫 번째 chunk 에서는 v1, v2 가, 두 번째 chunk 에서는 v3, v4 가 들어간다.
    # 실행 후 filtered_yn 플래그가 올바르게 세팅되는지 확인한다.
    async def run_all():
        await asyncio.gather(*tasks)

    asyncio.run(run_all())

    # 첫 번째 chunk (exist_stage=True) 에서는 v1 이 필터링돼야 함
    assert v1.filtered_yn is True
    assert v1.filtered_reason == VocFilterMessage.CH_STGE_NOT_RELATED.value
    # 두 번째 chunk (exist_stage=False) 에서는 v3 이 필터링돼야 함
    assert v3.filtered_yn is True
    assert v3.filtered_reason == VocFilterMessage.CH_STGE_NOT_RELATED.value

    # v2, v4 는 응답 성공이므로 그대로 False
    assert not v2.filtered_yn
    assert not v4.filtered_yn


# ----------------------------------------------------------------------
# 7️⃣ 전체 execute 흐름 (중복제거 → 필터링 → NLP) 테스트
# ----------------------------------------------------------------------
@pytest.mark.asyncio
async def test_execute_full_flow():

    voc_filter = make_voc_filter()
    
    # 1) 중복 PK 2개, 2) 필터링 대상(길이 짧은) 1개, 3) 정상 VOC 2개
    v1 = make_voc_info(group_co_cd="1", orin_voc_content="안 녕")            # 길이 2 → 필터링
    v2 = make_voc_info(                                                      # 정상
        group_co_cd="2", 
        orin_voc_content="로그인이 조금 느려요", 
        final_qsitm_pol_taget_dstcd="02", 
        cx_stge_dstcd="18", 
        cx_stge_dstic_nm="로그인/인증"
    )
    v3 = make_voc_info(group_co_cd="2", orin_voc_content="중복PK")            # 중복
    v4 = make_voc_info(                                                       # 관계 없는
        group_co_cd="3", 
        orin_voc_content="테스트테스트",
        final_qsitm_pol_taget_dstcd="02", 
        cx_stge_dstcd="00", 
    )

    vocs = [v1, v2, v3, v4]

    # async execute 호출
    result = await voc_filter.execute(vocs)

    # 1) 중복 처리
    assert v3.filtered_yn is True
    assert v3.filtered_reason == VocFilterMessage.DUPLICATE_PK.value

    # 2) 길이 필터링
    assert v1.filtered_yn is True
    assert v1.filtered_reason == VocFilterMessage.TEXT_FILTER.value

    # 3) NLP 필터링
    assert v4.filtered_yn is True
    assert v4.filtered_reason == VocFilterMessage.CH_STGE_NOT_RELATED.value
    
    # 4) 성공
    assert v2.filtered_yn is False
    

    # 최종 반환값은 원본 리스트와 동일한 객체 리스트이어야 함
    assert result is vocs


