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
    write_df_to_sheets,
    ### job 비동기 처리 함수
    run_sql_jobs,
    SQL2df_all,
    make_script_content,
    make_voc_summary,
    make_voc_compare,
    ### 날짜 처리 함수
    make_bu_nps_weekly_report_date_params,
)

#S1_04 raw 데이터를 붙여넣을 Excel 시트명 (템플릿 파일의 실제 시트명으로 설정)
S1_01_RAW_SHEET_NAME = 'BU_NPS_RAW'
S1_04_RAW_SHEET_NAME = 'NPS관리활동_RAW'
S1_05_RAW_SHEET_NAME = '고객경험단계별NPS_RAW'
# ----------------------------------------------------------------------------------------------------
# 메인 엔트리 – 전체 흐름
# ----------------------------------------------------------------------------------------------------
async def generate_bu_nps_weekly_report(mcp_executor, user_id, llm, prompts:dict, today_date) -> Path:
    val_dict = {} # 결과 적재용 dictionary

    ##################################################
    ### 0. 날짜 처리 및 값 저장
    ##################################################
    date_params = await make_bu_nps_weekly_report_date_params(mcp_executor)

    monday_b01w        = date_params.get('monday_b01w')
    biz_endday_b01w    = date_params.get('biz_endday_b01w')
    monday_b02w        = date_params.get('monday_b02w')
    biz_endday_b02w    = date_params.get('biz_endday_b02w')
    biz_endday_b01m    = date_params.get('biz_endday_b01m')
    yyyymmdd_cx_manage = date_params.get('yyyymmdd_cx_manage')

    cm_yyyy     = int(biz_endday_b01w[0:4])
    cm_yy       = int(biz_endday_b01w[2:4])
    cm_mm       = int(biz_endday_b01w[4:6])
    cm_dd       = int(biz_endday_b01w[6:8])
    cx_mm       = 3 if cm_yyyy == 2025 else 1
    cm_yyyymmdd = f'{cm_yyyy}.{cm_mm}.{cm_dd}'
    
    val_dict['cm_yyyy']     = cm_yyyy
    val_dict['cm_yy']       = cm_yy
    val_dict['cm_mm']       = cm_mm
    val_dict['cx_mm']       = cx_mm
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
        # Sheet 1.NPS대시보드
        ('S1_01'   , 'BU_WEEK_S1_01_RAW.sql', 'mysql_query' , date_params),
        ('S1_02'   , 'BU_WEEK_S1_02.sql', 'mysql_query' , date_params),
        ('S1_04'   , 'BU_WEEK_S1_04_RAW.sql', 'mysql_query' , date_params),
        ('S1_05'   , 'BU_WEEK_S1_05_RAW.sql', 'mysql_query' , date_params),
    
        # Sheet 2.NSS,CCI대시보드
        ('S2_01'   , 'BU_WEEK_S2_01.sql', 'mysql_query' , date_params),
        ('S2_02'   , 'BU_WEEK_S2_02.sql', 'mysql_query' , date_params),
        ('S2_03'   , 'BU_WEEK_S2_03.sql', 'mysql_query' , date_params),
        ('S2_04'   , 'BU_WEEK_S2_04.sql', 'mysql_query' , date_params),
    
        # Sheet 3.VOC대시보드
        ('S3_01_01', 'BU_WEEK_S3_01.sql', 'mysql_query', {**date_params, 'channel': 'KB 스타뱅킹'}),
        ('S3_01_02', 'BU_WEEK_S3_01.sql', 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S3_01_03', 'BU_WEEK_S3_01.sql', 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S3_01_04', 'BU_WEEK_S3_01.sql', 'mysql_query', {**date_params, 'channel': '상품'}),
        ('S3_02_01', 'BU_WEEK_S3_02.sql', 'mysql_query', {**date_params, 'channel': 'KB 스타뱅킹'}),
        ('S3_02_02', 'BU_WEEK_S3_02.sql', 'mysql_query', {**date_params, 'channel': '영업점'}),
        ('S3_02_03', 'BU_WEEK_S3_02.sql', 'mysql_query', {**date_params, 'channel': '고객센터'}),
        ('S3_02_04', 'BU_WEEK_S3_02.sql', 'mysql_query', {**date_params, 'channel': '상품'}),
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
        ('S1_01', 'S1_script1', 'BU_NPS_phrase_prompt'),
        ('S2_01', 'S2_script1', 'BU_NSS_phrase_prompt'),
        ('S2_02', 'S2_script2', 'BU_CCI_phrase_prompt'),
    ]
    
    # 2-2) VOC 주요 불만사항 및 예시 추출
    """
    tuple:
    1. df_dict에서 사용할 df key이름
    2. 활용 프롬프트
    3. 프롬프트 내 channel 값 formatting
    """
    voc_summary_jobs = [
        ('S3_01_01', 'BU_VOC_summary_prompt', 'KB 스타뱅킹'),
        ('S3_01_02', 'BU_VOC_summary_prompt', '영업점'),
        ('S3_01_03', 'BU_VOC_summary_prompt', '고객센터'),
        ('S3_01_04', 'BU_VOC_summary_prompt', '상품'),
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
        ('S3_02_01', 0, 'BU_VOC_compare_prompt', 'KB 스타뱅킹'),
        ('S3_02_02', 1, 'BU_VOC_compare_prompt', '영업점'),
        ('S3_02_03', 2, 'BU_VOC_compare_prompt', '고객센터'),
        ('S3_02_04', 3, 'BU_VOC_compare_prompt', '상품'),
    ]

    ##################################################
    ### 2. job 실행 및 결과 저장
    ##################################################
    # 1. SQL job 실행 및 결과 저장
    #대용량으로 처리할 것들은 not in에 추가해야함
    sql_jobs_normal = [job for job in sql_jobs if job[0] not in ('S1_04')]
    sql_result = await run_sql_jobs(sql_jobs_normal, mcp_executor, SQL_PATH)
    df_dict: dict[str, pd.DataFrame] = dict(sql_result)

    #S1_04는 페이지네이션으로 전체 데이터 수집(추후 전체로 확장가능)
    df_dict['S1_04'] = await SQL2df_all(mcp_executor, SQL_PATH, 'BU_WEEK_S1_04_RAW.sql', 'mysql_query', date_params)
    
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
        if df_name == 'S1_04':
            # raw 데이터는 플레이스홀더 방식 대신 시트에 직접 붙여넣기
            continue
        col_prefix = df_name.split('_')[0]
        df.columns = [f'{col_prefix}_{col}' for col in df.columns]
        val_dict = append_val_to_dict(df, val_dict)

    ##################################################
    ### 3. 최종 Excel 파일 생성
    ##################################################
    input_file = f'{REPORT_TEMPLATE_PATH}/bu_nps_weekly_report_temp.xlsx'
    output_file = f'{OUTPUT_DIR}/bu_nps_weekly_report_{biz_endday_b01w}.xlsx'
    replace_placeholders_in_workbook(input_file, output_file, val_dict)

    #S1_04 raw 데이터를 지정 시트에 직접 붙여넣기
    write_df_to_sheets(output_file, [
        (df_dict['S1_01'], S1_01_RAW_SHEET_NAME),
        (df_dict['S1_04'], S1_04_RAW_SHEET_NAME),
        (df_dict['S1_05'], S1_05_RAW_SHEET_NAME),
    ])

# --------------------------------------------------
# 직접 실행용
# --------------------------------------------------
if __name__ == "__main__":
    asyncio.run(generate_bu_nps_weekly_report())