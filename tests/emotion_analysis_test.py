import asyncio
import sys
import os
import json
import argparse
import csv
import time

from typing import Any, Dict, List, Optional

from agent.data_analysis.tools.emotion_analysis import EmotionAnalyzer
from agent.data_analysis.model.dto import EmotionAnlaysisRequestInfo
from agent.data_analysis.model.vo import VocInfo, EmotionMappingInfo

from core.mcp_util import get_mcp_executor


async def main(base_ymd: str):

    # 1. Mock 데이터 세팅
    # 감정 대분류 딕셔너리 세팅
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
    # 감정 중분류 딕셔너리 세팅
    emotion_midium_dict = {
        '01': EmotionMappingInfo(
            code='01',
            name='감동',
            desc='원하는 바를 이루고 매우 기분이 좋은 상태',
            emotion_large_cd='01',
            voc_type_cd='01',
        ),
        '02': EmotionMappingInfo(
            code='02',
            name='만족',
            desc='원하는 바를 이룬 상태',
            emotion_large_cd='01',
            voc_type_cd='01',
        ),
        '03': EmotionMappingInfo(
            code='03',
            name='기대',
            desc='원하는 바를 이루어지기를 바라고 기다림',
            emotion_large_cd='01',
            voc_type_cd='03',
        ),
        '04': EmotionMappingInfo(
            code='04',
            name='아쉬움',
            desc='원하는 바를 이루어지지 않아 바라고 기다림',
            emotion_large_cd='02',
            voc_type_cd='03',
        ),
        '05': EmotionMappingInfo(
            code='05',
            name='실망',
            desc='원하는 바를 이루지 못한 상태',
            emotion_large_cd='02',
            voc_type_cd='02',
        ),
        '06': EmotionMappingInfo(
            code='06',
            name='짜증',
            desc='원하는 바를 이루지 못하고 매우 불쾌한 기분이나 상태',
            emotion_large_cd='02',
            voc_type_cd='02',
        ),
        '07': EmotionMappingInfo(
            code='07',
            name='감정 없음',
            desc='원하는 바가 없거나 긍 부정의 표현이 없는 경우',
            emotion_large_cd='03',
            voc_type_cd='04',
        )
    }

    # voc 추출
    voc_infos: List[Dict[str, Any]] = await search_voc_data(base_ymd)
    if len(voc_infos) == 0:
        print(f"감정 분석 대상 데이터가 없습니다. 기준년월: {base_ymd}")
        return

    req_info = EmotionAnlaysisRequestInfo(batch_size=150)
    
    emotion_analyzer = EmotionAnalyzer(
        req_info=req_info,
        emotion_midium_dict=emotion_midium_dict,
        emotion_large_dict=emotion_large_dict,
        voc_type_dict=voc_type_dict,
    )


    print(f"================================================================감정 분석 시작 VOC 데이터: {len(voc_infos)}건================================================================")
    start_time = time.time()
    await emotion_analyzer.execute(voc_infos)
    end_time = time.time()
    elapsed_time = start_time - end_time
    print(f"================================================================감성 분석 종료 소요 시간: {elapsed_time:.2f} 초================================================================")
    

    csv_rows = [
        [
            # key
            '그룹회사_코드', '기준년월일', '설문ID', '설문참여대상자고유ID', '문항ID', '정렬순서', 
            # 부가 정보
            '설문조사대상구분', '실제채널', '고객경험단계구분', '질문의도대구분',
            # 구체적 경험 VOC
            '추천이유문항제목', '추천이유문항설명', '추천이유문항응답내용',
            # VOC
            'VOC원문',
            # 결과
            'old_중분류_결과', 'old_대분류_결과', 'old_voc유형',
            'new_중분류_결과', 'new_대분류_결과', 'new_voc유형',
            'new_extend_중분류_결과', 'new_extend_대분류_결과', 'new_extend_voc유형'
        ]
    ]
    

    with open(f"{base_ymd}_output_cx_ch_detail.csv", "w", newline="", encoding="utf-8") as f:

        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        
        for voc_info in voc_infos:
            new_emotion_info = emotion_midium_dict.get(voc_info.new_emotion_midium_code)
            new_extend_emotion_info = emotion_midium_dict.get(voc_info.new_extend_emotion_midium_code)
            old_emotion_info = emotion_midium_dict.get(voc_info.old_emotion_midium_code)
    
            csv_row = [
                # key
                voc_info.group_co_cd, voc_info.base_ymd, voc_info.qusn_id, voc_info.qusn_invl_tagtp_uniq_id, voc_info.qsitm_id, voc_info.lnp_seq, 
                # 부가정보       
                voc_info.pol_taget_name, voc_info.final_ch_name, voc_info.cust_expr_stge_name, voc_info.qry_inten_lag_name,
                # 구체적 경험
                voc_info.rcmdn_resn_qsitm_name, voc_info.rcmdn_resn_qsitm_desc, voc_info.rcmdn_resn_qsitm_content,
                # VOC
                voc_info.voc_content,
            ]
            
            print("====================================================="*3)
            print(f" [그룹회사 코드:{voc_info.group_co_cd} | 기준년월일:{voc_info.base_ymd} | 설문ID: {voc_info.qusn_id} | 설문참여대상자고유ID: {voc_info.qusn_invl_tagtp_uniq_id} | 문항ID: {voc_info.qsitm_id} | 정렬순서: {voc_info.lnp_seq}]")
            print("====================================================="*3)
            print(f"VOC 원문: \n{voc_info.voc_content}")
            print("====================================================="*3)
            print(f"감정 분석 결과:\n")
            
            if old_emotion_info:
                csv_row.append(old_emotion_info.name)
                csv_row.append(emotion_large_dict.get(old_emotion_info.emotion_large_cd))
                csv_row.append(voc_type_dict.get(old_emotion_info.voc_type_cd))
                print(f"old: [중분류 결과: {old_emotion_info.name}, 대분류 결과: {emotion_large_dict.get(old_emotion_info.emotion_large_cd)}, VOC 유형 결과: {voc_type_dict.get(old_emotion_info.voc_type_cd)}]")
            else:
                csv_row.append("없음")
                csv_row.append("없음")
                csv_row.append("없음")
                print("old 감정 분류 결과 없음")

                
            if new_emotion_info:
                csv_row.append(new_emotion_info.name)
                csv_row.append(emotion_large_dict.get(new_emotion_info.emotion_large_cd))
                csv_row.append(voc_type_dict.get(new_emotion_info.voc_type_cd))
                print(f"new: [중분류 결과: {new_emotion_info.name}, 대분류 결과: {emotion_large_dict.get(new_emotion_info.emotion_large_cd)}, VOC 유형 결과: {voc_type_dict.get(new_emotion_info.voc_type_cd)}]")
            else:
                csv_row.append("감정분류_중_오류_발생")
                csv_row.append("감정분류_중_오류_발생")
                csv_row.append("감정분류_중_오류_발생")
                print("new 감정 분류 중 오류 발생")

                
            if new_extend_emotion_info:
                csv_row.append(new_extend_emotion_info.name)
                csv_row.append(emotion_large_dict.get(new_extend_emotion_info.emotion_large_cd))
                csv_row.append(voc_type_dict.get(new_extend_emotion_info.voc_type_cd))
                print(f"new: [중분류 결과: {new_extend_emotion_info.name}, 대분류 결과: {emotion_large_dict.get(new_extend_emotion_info.emotion_large_cd)}, VOC 유형 결과: {voc_type_dict.get(new_extend_emotion_info.voc_type_cd)}]")
            else:
                csv_row.append("감정분류_중_오류_발생")
                csv_row.append("감정분류_중_오류_발생")
                csv_row.append("감정분류_중_오류_발생")
                print("new extend 감정 분류 중 오류 발생")

            csv_rows.append(csv_row)

        writer.writerows(csv_rows)

        

        


async def search_voc_data(base_ymd: str) -> List[VocInfo]:
    """
    주어진 기준년월일에 대해 VOC 데이터를 페이지 단위로 조회합니다.
    페이지가 더 이상 없을 때까지 반복 실행합니다.
    """

    print("======================================================================VOC 데이터 조회======================================================================")
    
    executor = await get_mcp_executor()
    
    voc_list: List[VocInfo] = []
    PAGE_SIZE = 500
    offset = 0  # 현재 페이지의 시작 rn

    while True:
        # rn 범위를 offset 기반으로 지정
        rn_start = offset + 1
        rn_end = offset + PAGE_SIZE

        query = f"""
            WITH
            /* 구체적 경험 voc 데이터를 미리 집계 */
            sv73_detail AS (
                SELECT
                      sv73.그룹회사코드
                    , sv73.설문응답종료년월일 AS 기준년월일
                    , sv73.설문ID
                    , sv73.설문참여대상자고유ID
                    , sv73.설문조사방식구분
                    , sv73.설문조사종류구분
                    , sv73.설문조사대상구분
                    , sv73.고객경험단계구분
                    , MAX(CASE WHEN sv73.질문의도대구분 = '09' THEN sv73.문항ID END) AS VOC문항ID
                    , MAX(CASE WHEN sv73.질문의도대구분 = '13' THEN sv73.문항ID END) AS 추천이유문항ID
                    , MAX(CASE WHEN sv73.질문의도대구분 = '13' THEN sv73.문항응답내용 END) AS 추천이유문항응답내용
                FROM    inst1.tsccvsv73 sv73
                WHERE   sv73.질문의도대구분 IN ('09','13')
                  AND   sv73.사용여부 = '1'
                  AND   sv73.설문응답종료년월일 = '{base_ymd}'
                GROUP BY 1,2,3,4,5,6,7,8
            ),
            
            /* 메인 데이터 – ROW_NUMBER 로 페이징 */
            base_data AS (
                SELECT
                      mg57.그룹회사코드
                    , mg57.기준년월일
                    , mg57.설문ID
                    , mg57.설문참여대상자고유ID
                    , mg57.문항ID
                    , mg57.정렬순서
                    , mg57.고객경험정답감정코드
                    , mg57.질문의도대구분
                    , qryIntenLag.인스턴스내용 AS 질문의도대구분명
                    , sv73_detail.추천이유문항ID
                    , sv04_detail.문항제목명 AS 추천이유문항제목
                    , sv04_detail.문항설명내용 AS 추천이유문항설명
                    , sv73_detail.추천이유문항응답내용
                    , sv73_content.문항응답내용
                    , mg57.문항설문조사대상구분
                    , polTrg.인스턴스내용 AS 문항설문조사대상명
                    , mg57.고객경험단계구분
                    , cxStge.인스턴스내용 AS 고객경험단계명
                    , ROW_NUMBER() OVER (
                        ORDER BY mg57.그룹회사코드,
                                 mg57.설문ID,
                                 mg57.설문참여대상자고유ID,
                                 mg57.문항ID
                      ) AS rn
                FROM    inst1.TSCCVMG57 mg57
                INNER JOIN inst1.tsccvsv73 sv73_content
                        ON sv73_content.그룹회사코드 = mg57.그룹회사코드
                       AND sv73_content.설문ID = mg57.설문ID
                       AND sv73_content.설문참여대상자고유ID = mg57.설문참여대상자고유ID
                       AND sv73_content.문항ID = mg57.문항ID
                       AND sv73_content.설문응답종료년월일 = mg57.기준년월일
                LEFT JOIN sv73_detail
                        ON sv73_detail.그룹회사코드 = mg57.그룹회사코드
                       AND sv73_detail.설문ID = mg57.설문ID
                       AND sv73_detail.설문참여대상자고유ID = mg57.설문참여대상자고유ID
                       AND sv73_detail.VOC문항ID = mg57.문항ID
                       AND sv73_detail.기준년월일 = mg57.기준년월일
                       AND mg57.질문의도대구분 = '09'
                LEFT JOIN inst1.TSCCVSV11 sv11_detail
                         ON sv11_detail.그룹회사코드 = sv73_detail.그룹회사코드
                        AND sv11_detail.설문ID = sv73_detail.설문ID
                        AND sv11_detail.사용여부 = '1'
                LEFT JOIN inst1.TSCCVSV04 sv04_detail
                     ON sv04_detail.그룹회사코드 = sv73_detail.그룹회사코드
                    AND sv04_detail.설문양식ID = sv11_detail.설문양식ID
                    AND sv04_detail.문항ID = sv73_detail.추천이유문항ID
                    AND sv04_detail.사용여부 = '1'
                LEFT JOIN inst1.TSCCVCI04 qryIntenLag
                        ON qryIntenLag.그룹회사코드 = mg57.그룹회사코드
                       AND qryIntenLag.인스턴스식별자 = '142457000'
                       AND qryIntenLag.인스턴스코드 = mg57.질문의도대구분
                LEFT JOIN inst1.TSCCVCI04 polTrg
                        ON polTrg.그룹회사코드 = mg57.그룹회사코드
                       AND polTrg.인스턴스식별자 = '142447000'
                       AND polTrg.인스턴스코드 = mg57.문항설문조사대상구분
                LEFT JOIN inst1.TSCCVCI04 cxStge
                        ON cxStge.그룹회사코드 = mg57.그룹회사코드
                       AND cxStge.인스턴스식별자 = '142594000'
                       AND cxStge.인스턴스코드 = mg57.고객경험단계구분
                WHERE 1=1
                  AND mg57.기준년월일 = '{base_ymd}'
                  AND mg57.정렬순서 = 1
            )
            
            SELECT *
            FROM   base_data
            WHERE  1=1
              AND rn BETWEEN {rn_start} AND {rn_end}
            ORDER BY rn
            ;
        """

        print(f"쿼리 (페이지 {offset // PAGE_SIZE + 1}):\n{query}")

        # 설문 문항 조회
        search_result = await executor.execute_tool("mysql_query", {"query": query})
        print(f"페이지 {offset // PAGE_SIZE + 1} 결과 수: {len(search_result)}")

        # 더 이상 결과가 없으면 종료
        if not isinstance(search_result, list) or len(search_result) == 0:
            break

        # voc list vo 세팅
        for search_dict in search_result:
            voc_content = search_dict.get('문항응답내용')
            if not voc_content:
                # voc 원문이 없는 빈경우 제외
                continue

            pol_taget_dstcd = search_dict['문항설문조사대상구분']
            cust_expr_stge_dstcd = search_dict['고객경험단계구분']
            pol_taget_name=search_dict['문항설문조사대상명']
            
            final_ch_name = pol_taget_name
            if pol_taget_dstcd == '07' or pol_taget_dstcd == '06':
                if cust_expr_stge_dstcd == '07' or cust_expr_stge_dstcd == '08' or cust_expr_stge_dstcd == '09' or cust_expr_stge_dstcd == '10' or cust_expr_stge_dstcd == '11' or cust_expr_stge_dstcd == '12':
                    final_ch_name = '상품'
            
            
            voc_info = VocInfo(
                # keys
                group_co_cd=search_dict['그룹회사코드'],
                base_ymd=search_dict['기준년월일'],
                qusn_id=search_dict['설문ID'],
                qusn_invl_tagtp_uniq_id=search_dict['설문참여대상자고유ID'],
                qsitm_id=search_dict['문항ID'],
                # 추천이유 -> 구체적경험 VOC
                rcmdn_rson_qsitm_id=search_dict.get('추천이유문항ID'),
                rcmdn_resn_qsitm_name=search_dict.get('추천이유문항제목'),
                rcmdn_resn_qsitm_desc=search_dict.get('추천이유문항설명'),
                rcmdn_resn_qsitm_content=search_dict.get('추천이유문항응답내용'),
                # VOC
                orin_voc_content=voc_content,
                cust_expr_stge_dstcd=cust_expr_stge_dstcd,
                cust_expr_stge_name=search_dict['고객경험단계명'],
                pol_taget_dstcd=pol_taget_dstcd,
                pol_taget_name=pol_taget_name,
                final_ch_name=final_ch_name,
                qry_inten_lag_dstcd=search_dict['질문의도대구분'],
                qry_inten_lag_name=search_dict['질문의도대구분명'],
                # Regacy 비교
                old_emotion_midium_code=search_dict['고객경험정답감정코드']
            )
            voc_list.append(voc_info)

        # 다음 페이지로 이동
        offset += PAGE_SIZE

    print("======================================================================VOC 데이터 조회 끝======================================================================")
    return voc_list
        
    

if __name__ == "__main__":

    # 인자값 세팅
    parser = argparse.ArgumentParser(description="기준년월일 입력받기")
    parser.add_argument("text", help="감정 분석할 기준년월일")

    args = parser.parse_args()
    if not args.text:
        print("분석 대상 기준년월일을 입력해주세요.")
    else:
        asyncio.run(main(args.text))