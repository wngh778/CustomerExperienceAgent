# 보고서 생성 에이전트

## Intro

```
app/agent/report_generation/
├─ reports/
│    ├─ config/
│    │    └─ report.yaml                            # Top-down 보고서용 내용 - 쿼리 매핑
│    │
│    ├─ resources/
│    │    ├─ report_template/                       # 입력 보고서 템플릿 문서
│    │    ├─ sql/                                   # 입력 쿼리 파일
│    │    └─ prompt/                                # 입력 프롬프트 파일
│    │
│    ├─ report_types/
│    │    ├─ td_nps_report.py                       # 1. Top-down NPS 보고서 생성
│    │    ├─ bu_nps_weekly_report.py                # 2. Bottom-up NPS 주간 보고서 생성
│    │    ├─ bu_nps_monthly_report.py               # 3. Bottom-up NPS 월간 보고서 생성
│    │    ├─ region_group_nps_biweekly_report.py    # 4. 지역영업그룹 NPS 격주 보고서 생성
│    │    └─ utils.py                               # 보고서 생성 유틸 함수
│    │
│    └─ README.MD
│
├─ document_report_agent.py                         # 보고서 생성 에이전트 정의
└─ __init__.py                                      # 보고서 생성 에이전트 import
```


## [INPUT]
```
app/agent/report_generation/reports/resources/report_template/
app/agent/report_generation/reports/resources/sql/
app/agent/report_generation/reports/resources/prompt/
```

## [OUTPUT]
```
app/output/report/
```

1. Top-down NPS 보고서 (배치주기: Top-down 보고서 결과 데이터 적재시)
```
전체: td_nps_report_{yyyy}_{반기}_all.docx, md
은행: td_nps_report_{yyyy}_{반기}_bank.docx, md
브랜드: td_nps_report_{yyyy}_{반기}_brand.docx, md
플랫폼: td_nps_report_{yyyy}_{반기}_platform.docx, md
대면채널: td_nps_report_{yyyy}_{반기}_face.docx, md
고객센터: td_nps_report_{yyyy}_{반기}_center.docx, md
상품: td_nps_report_{yyyy}_{반기}_product.docx, md
```

2. Bottom-up NPS 주간 보고서 (배치주기: 매주 첫 영업일)
```
bu_nps_weekly_report_{yyyymmdd}.xlsx
```

3. Bottom-up NPS 월간 보고서 (배치주기: 매월 첫 영업일)
```
bu_nps_monthly_report_{yyyymm}.xlsx
```

4. 지역영업그룹 NPS 격주 보고서 (배치주기: 매격주 첫 영업일)
```
region_group_nps_biweekly_report_({지역영업그룹명})_{yyyymmdd}.xlsx
```


## 날짜 생성 흐름

### 1. **Top-down NPS 보고서**
```
마스터테이블(TSCCVBM11)에 적재된 데이터 기준 가장 최신 조사년도와 반기구분 값 활용

yyyy        : 최근 TD 조사년도
yyyyhf      : 최근 TD 조사반기
yyyy_b1hf   : 전반기 TD 조사년도
yyyyhf_b1hf : 전반기 TD 조사반기
```

### 2. **Bottom-up NPS 주간 보고서**
```
경영정보일자기본(TSCCVCI12) 테이블을 활용하여 주별 마지막 영업일 값을 추출하여 활용

monday_b01w        : 1주일 전 월요일에 대한 날짜 (yyyymmdd 형태)
biz_endday_b01w    : 1주일 전 마지막 영업일에 대한 날짜 (yyyymmdd 형태)
monday_b02w        : 2주일 전 월요일에 대한 날짜 (yyyymmdd 형태)
biz_endday_b02w    : 2주일 전 마지막 영업일에 대한 날짜 (yyyymmdd 형태)
biz_endday_b01m    : 1달 전 마지막 영업일에 대한 날짜 (yyyymmdd 형태)
yyyymmdd_cx_manage : 고객경험 관리 시작일자 (yyyymmdd 형태) / 2025년의 경우 3월부터로 지정되어 있어서 추가한 변수로 추후에는 불필요
```

### 3. **Bottom-up NPS 월간 보고서**
```
python datetime 라이브러리를 활용하여 날짜 계산 및 활용

yyyy               : 현재 연도 (yyyy 형태)
yyyy01             : 현재 연도의 1월 (yyyymm 형태)
yyyy0101           : 현재 연도의 1월 1일 (yyyymmdd 형태)
yyyymm_b01m        : 1달 전 날짜 (yyyymm 형태)
yyyymm_b02m        : 2달 전 날짜 (yyyymm 형태)
yyyymm_cx_manage : 고객경험 관리 시작일자 (yyyymm 형태) / 2025년의 경우 3월부터로 지정되어 있어서 추가한 변수로 추후에는 불필요
```

### 4. **지역영업그룹 NPS 격주 보고서**
```
python datetime 라이브러리를 활용하여 날짜 계산 및 활용

yyyy        : 현재 연도 (yyyy 형태)
yyyy0101    : 현재 연도의 1월 1일 (yyyymmdd 형태)
monday_b01w : 1주일 전 월요일에 대한 날짜 (yyyymmdd 형태)
friday_b01w : 1주일 전 금요일에 대한 날짜 (yyyymmdd 형태)
monday_b02w : 2주일 전 월요일에 대한 날짜 (yyyymmdd 형태)
friday_b03w : 3주일 전 금요일에 대한 날짜 (yyyymmdd 형태)
yyyymm_b01m : 지난 달 마지막 일 (yyyymmdd 형태)
```


## 실행 흐름

### 1. word 문서 생성 (`td_nps_report.py`)

1. **쿼리 로딩**
   - `reports/resources/sql/` 에서 `query_id`에 해당하는 SQL 파일을 읽어와서 replace


2. **프롬프트 로딩**
   - `reports/resources/prompt/` 에서 `prompt_file`을 읽어와서 replace


3. **쿼리 호출**
   - 비동기로 쿼리를 전부 조회하여 메모리에 저장


4. **LLM 호출**
   - 비동기로 LLM을 호출하여 개별 쿼리 결과+프롬프트를 LLM에게 전달하여 보고서 생성


5. **청크별 파일 저장**
   - `app/core/output/report/{report_name}/{cat_name}_{depth_name}_{chunk}.md` 로 저장


6. **보고서 병합**
   - 모든 챕터를 순서대로 이어붙여 하나의 Markdown 파일(`report_name.md`) 생성


7. **최종 파일 생성**
   - `md_to_docx_advanced`을 이용해 `report_name.md` → `report_name.docx` 변환


### 2. xlsx 문서 생성 (`bu_nps_weekly_report.py`, `bu_nps_monthly_report.py`, `region_group_nps_biweekly_report.py`)

1. **날짜 처리 및 값 저장**
   - `reports/report_types/utils` 에서 쿼리 조회에 필요한 날짜를 생성하는 함수 호출  


2. **job 정의**
   - 각 job(SQL 조회, LLM 생성 등)에 필요한 사전 정보 정의  


3. **job 실행 및 결과 저장**
   - 각 job을 ~~비동기~~ -> 동기로 실행하여 `val_dict` 에 key: value 형태로 저장  


4. **최종 Excel 파일 생성**
   - `reports/resources/report_template/` 에서 보고서 템플릿을 호출하고 `val_dict`에 있는 값으로 placeholer 하여 최종 보고서 초안 파일 생성  
  
## 추후 변경 필요사항

### 1. 현재, 아래 두 가지 이유로 추후 스크립트 수정 필요
   - 마스터테이블이 sqlite에 적재, 추후 운영시 mysql에 마스터테이블 적재 예정
   - 마스터테이블 데이터 부족으로 mysql에서 CCV 테이블 직접 조회  


### 2. 수정 필요 내용

1. 일부 WITH문으로 마스터테이블을 mysql에서 조회하는 쿼리 삭제 필요
- `app/agent/report_generation/reports/resources/sql/`의 SQL

```
(AS-IS)

WITH TSCCVBM11 AS (
    SELECT ~~
)
SELECT ~
FROMTSCCVBM11

(TO-BE)

SELECT ~
FROM INST1.TSCCVBM11
```

2. 조회 DB 지정을 sqlite에서 mysql로 수정
- `app/agent/report_generation/reports/resources/report_types/bu_nps_weekly_report.py`의 sql jobs 리스트 안의 값
- `app/agent/report_generation/reports/resources/report_types/bu_nps_monthly_report.py`의 sql jobs 리스트 안의 값
- `app/agent/report_generation/reports/resources/report_types/region_group_nps_biweekly_report.py`의 sql jobs 리스트 안의 값
- `app/agent/report_generation/reports/resources/report_types/utils.py`의 245, 1055행