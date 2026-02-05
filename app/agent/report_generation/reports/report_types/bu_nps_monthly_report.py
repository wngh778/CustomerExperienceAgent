import asyncio
import pandas as pd
from pathlib import Path
from .utils import (
    ### 경로 설정
    PROMPT_PATH,
    SQL_PATH,
    REPORT_TEMPLATE_PATH,
    OUTPUT_DIR,
    ### 보고서 생성 관련 함수
    append_val_to_dict,
    replace_placeholders_in_workbook,
    ### job 비동기 처리 함수
    run_sql_jobs,
    make_script_content,
    make_voc_summary,
    make_voc_compare,
    ### 날짜 처리 함수
    make_bu_nps_monthly_report_date_params
)
# ----------------------------------------------------------------------------------------------------
# 메인 엔트리 – 전체 흐름
# ---------------------------------------------------------------------------------------------------- 
async def generate_bu_nps_monthly_report(mcp_executor, user_id, llm, prompts:dict, today_date) -> Path:
    val_dict = {} # 결과 적재용 dictionary

    ##################################################
    ### 0. 날짜 처리 및 값 저장
    ##################################################
    date_params = make_bu_nps_monthly_report_date_params()

    yyyy             = date_params.get('yyyy')
    yyyy01           = date_params.get('yyyy01')
    yyyy0101         = date_params.get('yyyy0101')
    yyyymm_b01m      = date_params.get('yyyymm_b01m')
    yyyymmdd_b01m    = date_params.get('yyyymmdd_b01m')
    yyyymm_cx_manage = date_params.get('yyyymm_cx_manage')

    cm_yyyy     = int(yyyymmdd_b01m[0:4])
    cm_yy       = int(yyyymmdd_b01m[2:4])
    cm_mm       = int(yyyymmdd_b01m[4:6])
    cm_dd       = int(yyyymmdd_b01m[6:8])
    cm_yyyymmdd = f'{cm_yyyy}.{cm_mm}.{cm_dd}'
    
    val_dict['cm_yy']       = cm_yy
    val_dict['cm_mm']       = cm_mm
    val_dict['cm_yyyymmdd'] = cm_yyyymmdd

    ##################################################
    ### 1. job 정의
    ##################################################
    ### 1. SQL job
    """
    tuple:
    1. df_dict에 저장할 df key이름
    2. SQL 파일명
    3. DB 종류
    4. 입력 파라미터
    """
    sql_jobs = [
        # Sheet 1.NPS 현황 요약
        ('S1_01'    , 'BU_MONTH_S1_01.sql' , 'mysql_query' , date_params),
        ('S1_02'    , 'BU_MONTH_S1_02.sql' , 'mysql_query', date_params),
        ('S1_03'    , 'BU_MONTH_S1_03.sql' , 'mysql_query', date_params),
        ('S1_04'    , 'BU_MONTH_S1_04.sql' , 'mysql_query', date_params),
        ('S1_05'    , 'BU_MONTH_S1_05.sql' , 'mysql_query' , date_params),
    
        # Sheet 2.종합 NPS 현황
        ('S2_01'    , 'BU_MONTH_S2_01.sql' , 'mysql_query', date_params),
        ('S2_02'    , 'BU_MONTH_S2_02.sql' , 'mysql_query', date_params),
    
        # Sheet 3.스타뱅킹 NPS 현황
        ('S3_01'    , 'BU_MONTH_S3_01.sql' , 'mysql_query', {**date_params, 'channel': '스타뱅킹'}),
        ('S3_02'    , 'BU_MONTH_S3_02.sql' , 'mysql_query', {**date_params, 'channel': '스타뱅킹'}),
        ('S3_03'    , 'BU_MONTH_S3_03.sql' , 'mysql_query', {**date_params, 'channel': '스타뱅킹'}),
        ('S3_04'    , 'BU_MONTH_S3_04.sql' , 'mysql_query', {**date_params, 'channel': '스타뱅킹'}),
    
        # Sheet 4.영업점 NPS 현황
        ('S4_01'    , 'BU_MONTH_S3_01.sql' , 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S4_02'    , 'BU_MONTH_S3_02.sql' , 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S4_03'    , 'BU_MONTH_S3_03.sql' , 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S4_04'    , 'BU_MONTH_S3_04.sql' , 'mysql_query', {**date_params, 'channel': '영업점'}),
    
        # Sheet 5.고객센터 NPS 현황
        ('S5_01'    , 'BU_MONTH_S3_01.sql' , 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S5_02'    , 'BU_MONTH_S3_02.sql' , 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S5_03'    , 'BU_MONTH_S3_03.sql' , 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S5_04'    , 'BU_MONTH_S3_04.sql' , 'mysql_query', {**date_params, 'channel': '고객센터'}),
    
        # Sheet 6.상품 NPS 현황
        ('S6_01'    , 'BU_MONTH_S3_01.sql' , 'mysql_query', {**date_params, 'channel': '상품'}),
        ('S6_02'    , 'BU_MONTH_S3_02.sql' , 'mysql_query', {**date_params, 'channel': '상품'}),
        ('S6_03'    , 'BU_MONTH_S3_03.sql' , 'mysql_query', {**date_params, 'channel': '상품'}),
        ('S6_04'    , 'BU_MONTH_S3_04.sql' , 'mysql_query', {**date_params, 'channel': '상품'}),
    
        # Sheet 7.NSS
        ('S7_01'    , 'BU_MONTH_S7_01.sql' , 'mysql_query', date_params),
        ('S7_02'    , 'BU_MONTH_S7_02.sql' , 'mysql_query', date_params),
    
        # Sheet 8.CCI
        ('S8_01'    , 'BU_MONTH_S8_01.sql' , 'mysql_query', date_params),
        ('S8_02'    , 'BU_MONTH_S8_02.sql' , 'mysql_query', date_params),
    
        # Sheet 9.종합 고객경험 관리활동 현황
        ('S9_01'    , 'BU_MONTH_S9_01.sql' , 'mysql_query' , date_params),
    
        # Sheet 10.채널별 고객경험 관리활동 현황
        ('S10_01'   , 'BU_MONTH_S10_01.sql', 'mysql_query' , date_params),
        ('S10_02'   , 'BU_MONTH_S10_02.sql', 'mysql_query', date_params),
    
        # Sheet 11.불만 VOC 요약 및 비교
        ('S11_01_01', 'BU_MONTH_S11_01.sql', 'mysql_query', {**date_params, 'channel': 'KB 스타뱅킹'}),
        ('S11_01_02', 'BU_MONTH_S11_01.sql', 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S11_01_03', 'BU_MONTH_S11_01.sql', 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S11_01_04', 'BU_MONTH_S11_01_04.sql', 'mysql_query', {**date_params, 'channel': '상품'}),
        ('S11_02_01', 'BU_MONTH_S11_02.sql', 'mysql_query', {**date_params, 'channel': 'KB 스타뱅킹'}),
        ('S11_02_02', 'BU_MONTH_S11_02.sql', 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S11_02_03', 'BU_MONTH_S11_02.sql', 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S11_02_04', 'BU_MONTH_S11_02_04.sql', 'mysql_query', {**date_params, 'channel': '상품'}),
    ]

    ### 2) LLM job
    # 2-1) 정량적 데이터 결과 기반의 인사이트 문구 생성
    """
    tuple:
    1. df_dict에서 사용할 df key이름
    2. xlsx 템플릿의 변수명으로 실제 LLM 생성값이 삽입되는 부분
    3. 활용 프롬프트
    """
    script_content_jobs = [
        ('S1_02', 'S1_script1', 'BU_NPS_phrase_prompt'),
        ('S1_03', 'S1_script2', 'BU_NSS_phrase_prompt'),
        ('S1_04', 'S1_script3', 'BU_CCI_phrase_prompt'),
        ('S1_05', 'S1_script4', 'BU_VOC_manage_phrase_prompt'),
        ('S2_01', 'S2_script1', 'BU_NPS_phrase_prompt'),
        ('S2_02', 'S2_script2', 'BU_NPS_group_phrase_prompt'),
        ('S1_02', 'S2_script3', 'BU_NPS_phrase_prompt'),
        ('S3_01', 'S3_script1', 'BU_NPS_phrase_prompt'),
        ('S3_02', 'S3_script2', 'BU_NPS_group_phrase_prompt'),
        ('S3_03', 'S3_script3', 'BU_NPS_imp_phrase_prompt'),
        ('S3_04', 'S3_script4', 'BU_Detractor_phrase_prompt'),
        ('S4_01', 'S4_script1', 'BU_NPS_phrase_prompt'),
        ('S4_02', 'S4_script2', 'BU_NPS_group_phrase_prompt'),
        ('S4_03', 'S4_script3', 'BU_NPS_imp_phrase_prompt'),
        ('S4_04', 'S4_script4', 'BU_Detractor_phrase_prompt'),
        ('S5_01', 'S5_script1', 'BU_NPS_phrase_prompt'),
        ('S5_02', 'S5_script2', 'BU_NPS_group_phrase_prompt'),
        ('S5_03', 'S5_script3', 'BU_NPS_imp_phrase_prompt'),
        ('S5_04', 'S5_script4', 'BU_Detractor_phrase_prompt'),
        ('S6_01', 'S6_script1', 'BU_NPS_phrase_prompt'),
        ('S6_02', 'S6_script2', 'BU_NPS_group_phrase_prompt'),
        ('S6_03', 'S6_script3', 'BU_NPS_imp_phrase_prompt'),
        ('S6_04', 'S6_script4', 'BU_Detractor_phrase_prompt'),
        ('S7_01', 'S7_script1', 'BU_NSS_phrase_prompt'),
        ('S7_02', 'S7_script2', 'BU_NSS_group_phrase_prompt'),
        ('S1_03', 'S7_script3', 'BU_NSS_phrase_prompt'),
        ('S8_01', 'S8_script1', 'BU_CCI_phrase_prompt'),
        ('S8_02', 'S8_script2', 'BU_CCI_group_phrase_prompt'),
        ('S1_04', 'S8_script3', 'BU_CCI_phrase_prompt'),
        ('S9_01', 'S9_script1', 'BU_VOC_manage_phrase_prompt'),
    ]
    
    # 2-2) VOC 주요 불만사항 및 예시 추출
    """
    tuple:
    1. df_dict에서 사용할 df key이름
    2. 활용 프롬프트
    3. 프롬프트 내 channel 값 formatting
    """
    voc_summary_jobs = [
        ('S11_01_01', 'BU_VOC_summary_prompt', 'KB 스타뱅킹'),
        ('S11_01_02', 'BU_VOC_summary_prompt', '영업점'),
        ('S11_01_03', 'BU_VOC_summary_prompt', '고객센터'),
        ('S11_01_04', 'BU_VOC_summary_prompt', '상품'),
    ]
    
    # 2-3) 이번 VOC 주요 불만사항 vs. 저번 VOC 비교
    """
    tuple:
    1. df_dict에서 사용할 df key이름
    2. 이전 단예 결과인 voc_summary에서 활용할 결과의 인덱스
    3. 활용 프롬프트
    4. 프롬프트 내 channel 값 formatting
    """
    voc_compare_jobs = [
        ('S11_02_01', 0, 'BU_VOC_compare_prompt', 'KB 스타뱅킹'),
        ('S11_02_02', 1, 'BU_VOC_compare_prompt', '영업점'),
        ('S11_02_03', 2, 'BU_VOC_compare_prompt', '고객센터'),
        ('S11_02_04', 3, 'BU_VOC_compare_prompt', '상품'),
    ]

    ##################################################
    ### 2. job 실행 및 결과 저장
    ##################################################
    # 1. SQL job 실행 및 결과 저장
    sql_result = await run_sql_jobs(sql_jobs, mcp_executor, SQL_PATH)
    df_dict: dict[str, pd.DataFrame] = dict(sql_result)

    # 2. LLM job 실행 및 결과 저장
    # 2-1) 정량적 데이터 결과 기반의 인사이트 문구 생성
    script_content = await make_script_content(script_content_jobs, df_dict, user_id, llm, PROMPT_PATH)
    val_dict = val_dict | script_content

    # 2-2) VOC 주요 불만사항 및 예시 추출
    voc_summary = await make_voc_summary(voc_summary_jobs, df_dict, user_id, llm, PROMPT_PATH, 'BU_VOC')
    df_vs_name = '_'.join(voc_summary_jobs[0][0].split('_')[:-1])
    df_dict[df_vs_name] = pd.concat(voc_summary).reset_index(drop=True)

    # 2-3) 이번 VOC 주요 불만사항 vs. 저번 VOC 비교
    voc_compare = await make_voc_compare(voc_compare_jobs, voc_summary, df_dict, user_id, llm, PROMPT_PATH)
    df_vc_name = '_'.join(voc_compare_jobs[0][0].split('_')[:-1])
    df_dict[df_vc_name] = pd.DataFrame(voc_compare, columns=['compare'])

    for df_name, df in df_dict.items():
        col_prefix = df_name.split('_')[0]
        df.columns = [f'{col_prefix}_{col}' for col in df.columns]
        val_dict = append_val_to_dict(df, val_dict)

    ##################################################
    ### 3. 최종 Excel 파일 생성
    ##################################################
    input_file = f'{REPORT_TEMPLATE_PATH}/bu_nps_monthly_report_temp.xlsx'
    output_file = f'{OUTPUT_DIR}/bu_nps_monthly_report_{yyyymm_b01m}.xlsx'
    replace_placeholders_in_workbook(input_file, output_file, val_dict)

# ----------------------------------------------------------------------------------------------------
# 직접 실행용
# ----------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(generate_bu_nps_monthly_report())