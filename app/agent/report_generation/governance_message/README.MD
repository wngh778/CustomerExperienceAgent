# 거버넌스 생성 에이전트

## Intro
```
app/agent/report_generation/
|    ├── __init__.py                                 # 거버넌스 생성 에이전트 import
│    └── governance_message/
│        ├── README.MD
│        ├── __init__.py
│        ├── governance_message.py                   # 거버넌스 실행 main entry
│        ├── resources/
│        │   └── config
│        │        └── governance_screens.yaml        # 거버넌스 Depth별 내용 - 쿼리/템플릿/코드 맵핑
│        │   └── query
│        │        └── *.sql                          # 거버넌스 유형별 쿼리
│        │   └── templates
│        │        └── *.j2                           # 거버넌스 유형별 템플릿
│        │   └── utils
│        │        ├── __init__.py
│        │        ├── renderers.py                   # 거버넌스 유형별 랜더링
│________│________└── utils.py                       # 거버넌스 공통 함수 정의
```
---
---


## [INPUT]
1. `yaml 경로` 및 `코드 내 위치`
- app/agent/report_generation/governance_message/resources/config/governance_screens.yaml
- app/agent/report_generation/governance_message/resources/utils/utils.py 내 `load_screen_config`
<br/>

2. `쿼리 경로` 및 `코드 내 위치`
- app/agent/report_generation/governance_message/resources/query/*
- app/agent/report_generation/governance_message/resources/utils/utils.py 내 `get_query_file`에서 읽음.
<br/>

3. `템플릿 경로` 및 `코드 내 위치`
- app/agent/report_generation/governance_message/resources/templates/*
- app/agent/report_generation/governance_message/resources/utils/utils.py 내 `JINJA_ENV` 변수 설정.
- app/agent/report_generation/governance_message/resources/utils/rebderers.py 내 `JINJA_ENV`에서 읽음.
<br/>


## [OUTPUT]
```
app/output/governance/
```

1. Top-Down 유형
```
TD_요약_NPS진단 : "gvrnDstcd": "73",
TD_요약_문제영역 : "gvrnDstcd": "74"
TD_NPS진단_NPS진단 : "gvrnDstcd": "83"
TD_NPS진단_NPS영향요인 : "gvrnDstcd": "04"
TD_고객경험_NPS진단 : "gvrnDstcd": "84"
TD_고객경험_고객경험_NPS진단 : "gvrnDstcd": "A1/B1/C1/..."
TD_고객경험_고객경험_영향요인 : "gvrnDstcd": "A2/B2/C2/..."
TD_고객경험_고객경험_고객감정 : "gvrnDstcd": "A4"
TD_문제영역 : "gvrnDstcd": "80"
```

2. Bottom-up 유형
```
BU_요약_NPS진단 : "gvrnDstcd": "73"
BU_채널NPS_NPS분석 : "gvrnDstcd": "95"
BU_고객경험_고객경험_NPS분석 : "gvrnDstcd": "A1/B1/C1/..."
BU_고객경험_고객경험_품질요소 : "gvrnDstcd": "A3/B3/C3/..."
BU_문제원인 : "gvrnDstcd": "72"
```
---
---


## 실행 흐름 - governance_message.py<br/> [`app/agent/report_generation/governance_message/governance_message.py`]

1. **쿼리 결과 수집** - `_iterate_query_results`
   - 1-1. yaml 파일 읽어오기 - `load_screen_config`
   - 1-2. leaf_path 돌면서 query 수집 - `_collect_all_query_paths`
   - 1-3. cfg을 활용하여 leaf-query 수집 - `find_screen_by_path`
   - 1-4. 쿼리 비동기 실행 - `_run_one`
   - 1-5. leaf_path 단위로 쿼리 결과값 재정의 - `raw_results`

2. **템플릿 호출** - `generate_output`
   - 2-1. leaf_path 돌면서 템플릿 호출 - `render_template`

3. **content 객체 생성** - `_build_content_item`
   - 3-1. API response format 중 page 값에 들어갈 item 전달 (baseYm / polModDstcd / gvrnMsgCtnt 등)

4. **content 객체 생성**
   - API Response 내에 content 항목에 들어갈 내용 생성(baseYm / polModDstcd / gvrnMsgCtnt 등)

5. **페이지 객체 생성**  
   - API Response 내에 page 항목에 들어갈 내용 생성(pageNumber / totalElements 등) + content 객체

6. **최종 API 응답 생성**  
   - 메타데이터 `prcssYmYMS(yyyymmddhhmmss) + page(page + content)`


## 유지보수
1. Update) 해당하는 쿼리 수정 [`app/agent/report_generation/governance_message/resources/query/`]
   - [app/agent/report_generation/resources] `해당 쿼리`

2. Update) 해당하는 템플릿 수정 [`app/agent/report_generation/governance_message/resources/templates/`]
   - Jinja2 형식으로 해당하는 파일의 ` 해당 템플릿`

3. Update) 해당 랜더링 수정 [`app/agent/report_generation/governance_message/resources/utils/renderers.py`]
   - 각 보고서에 해당하는 `해당 랜더링`

4. Create) 해당 leaf 작성 [`app/agent/report_generation/governance_message/resources/config/`]
(위의 1번, 2번, 3번 항목 포함)
   - Hierarchy : <br/>
    `screens` > `보고서 유형 (top_down / bottom_up)` > `채널(depth3)` > `유형(depth4)`  > `상세유형(depth5)`



## Next Step
- SQLite -> MySQL 테이블 생성 시 쿼리 [`app/agent/report_generation/governance_message/resources/query/`] 업데이트

- Depth 구분하는 `_code` 분류 구체화 [`app/agent/report_generation/governance_message/resources/config/governance_screens.yaml`]

- 거버넌스 API 생성시 pagination 구체화 [`app/agent/report_generation/governance_message/governance_message.py`]
   - `_build_page` 내 항목들