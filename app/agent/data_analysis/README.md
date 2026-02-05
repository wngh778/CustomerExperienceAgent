# 데이터 분석 에이전트

## 1. Structure

```
app/agent/data_analysis/
│    ├── README.md
│    ├── __init__.py                                # 데이터분석 에이전트 import
│    ├── data_analysis_agent.py                     # 데이터분석 에이전트 최상위
│    ├── utils.py                                   # 데이터분석 하위 Agent들이 공통으로 사용하는 Util Class 
│    ├── model/
│    │   ├── __init__.py
│    │   ├── dto.py                                 # 데이터분석 에이전트에 쓰이는 DTO 클래스들
│    │   ├── vo.py                                  # 데이터분석 에이전트에 쓰이는 VO 클래스들
│    │   └── consts.py                              # 데이터분석 에이전트에 쓰이는 상수 클래스들
│    │
│    ├── tools/
│    │   ├── __init__.py
│    │   ├── voc_gathering/                         
│    │   │   ├── __init__.py
│    │   │   └── voc_gathering.py                   # 하위 Agent [VOC 수집]
│    │   ├── voc_filter/             
│    │   │   ├── __init__.py
│    │   │   └── voc_filter.py                      # 하위 Agent [VOC 필터] 
│    │   ├── cxe_mapping/        
│    │   │   ├── __init__.py
│    │   │   └── cxe_mapping.py                     # 하위 Agent [고객경험요소 매핑]
│    │   ├── emotion_analysis/   
│    │   │   ├── __init__.py
│    │   │   └── emotion_analysis.py                # 하위 Agent [감정분류]
│    │   ├── entity_word_detect/ 
│    │   │   ├── __init__.py
│    │   │   └── entity_word_detect.py              # 하위 Agent [개체어 식별]
│    │   └── voc_problem_reason_detect/
│    │       ├── __init__.py
│    │       └── voc_problem_reason_detect.py       # 하위 Agent [문제원인 식별]
│    │       
│    └── resources/
│            ├── sql/                                # 입력 쿼리 파일
│            └── prompt/                             # 입력 프롬프트 파일
```

## 2. 실행 흐름

```
[요청] ─> [VOC 수집] ─> [VOC 필터] ─> [고객경험요소 매핑] ─> [감정 분류]   ─> [문제원인 식별] ─> [응답]
                                                       └> [개체어 식별] ┘
```

### 1. 요청

- 데이터 분석 대상 페이지, 페이지 사이즈, 대상 년월일(YYYYMMDD), BatchSize를 입력 받는다.
    - 페이지(request.page), 페이지 사이즈(page_size): 분석할 VOC 데이터를 조회할 때, 페이징 관련 정보
    - 대상 년월일(request.date): 분석할 VOC를 조회할 날짜 (YYYYMMDD 포맷 문자열)
    - (Optional) 대상 종료 년월일(request.date_end): 분석할 VOC를 조회할 종료 일자
        - 있으면 조회할 때, BETWEEN 대상 년월일 AND 대상 종료 년월일
        - 없으면 조회할 때, 대상 년월일 하루치만 조회
    - 배치 사이즈(batch_size): LLM에 호출할 때, 비동기로 동시에 호출할 갯수

### 2. VOC 수집

- 일처리 대상 voc 중 데이터 분석 대상 voc 수집한다.
- 기능:
    1. 입력된 기준일자와 페이징 정보를 기반으로 분석할 대상 VOC를 조회한다.
        - `resources/sql/gathering_voc_response.sql`
    2. 채널/고객경험단계/고객경험요소/고객경험요소 관련 인스턴스 및 매핑 정보를 조회 후 앞으로 사용할 형태에 맞게 세팅한다.
        - `resources/sql/select_cxe_standard.sql`
    3. 감정 관련 인스턴스 및 매핑 정보를 조회 후 앞으로 사용할 형태에 맞게 세팅한다.
        - `resources/sql/select_emotion.sql`
    4. 이전 실행 개체어(`상품서비스용어`, `성능품질용어`) 결과들을 조회 후 앞으로 사용할 형태에 맞게 세팅한다.
        - `resources/sql/select_cxe_standard.sql`
    5. 1에서 조회된 VOC들의 제공/제외 여부를 체크한다. (불용어 포함 / 20음절 이하) (부점장/직원 제공 여부)
        - `resources/sql/select_exception_words.sql`
    6. VOC total count 조회
        - `resources/sql/gathering_voc_total_count.sql`
- 입력:
    - page, page_size: 조회 대상 페이지(request.page)와 페이지 사이즈(page_size)
    - start_ymd: 대상 년월일(request.date) 
    - end_ymd: 대상 종료 년월일(request.date_end), None이면, 대상 년월일(request.date) 값이 들어감(해당일만 조회)
    - pol_mod_cd_list: 조회할 설문조사방식구분 코드 리스트('01': TD, '02': BU), None이면 모든 방식이 조회된다.
    - ch_cd_list: 조회할 문항설문조사대상구분 코드 리스트 ('01': 브랜드 등등), None이면 모든 채널이 조회된다.
    - mcp_executor: mysql 및 sqlite 조회를 위한 실행기
    - prev_word_size: 이전 개체어 식별 결과를 조회할 때, 채널 별, 고객경험요소 별 각각 몇개씩 조회 해올지에 대한 사이즈. (기본 15개 씩)
- 출력:
    - Tuple [Index]: 설명
        - [0]: 분석 대상 VOC LIST
        - [1]: 채널/고객경험단계/고객경험요소/고객경험요소의 인스턴스 Dictionary
        - [2]: 채널 별, 고객경험단계명 인스턴스 리스트 Dictionary
        - [3]: 감정중분류 - 감정대분류 - VOC유형 인스턴스 Dictionary
        - [4]: 이전 실행 상품서비스용어 SET Dictionary (채널/고객경험요소 별)
        - [5]: 이전 실행 성능품질용어 SET Dictionary (채널/고객경험요소 별)
        - [6]: 분석 대상 VOC TotalCount
    - 각각의 voc_info VO의 멤버변수
        - brnmgr_ofer_yn: 부점장 제공 여부
        - emp_ofer_yn: 직원 제공 여부
        - text_eclud_resn_ctnt: 텍스트제외사유내용

### 3. VOC 필터

- 일처리 대상 voc 중 분석대상을 식별(일정 음절 미만 voc 삭제 등) 한다.
- 기능:
    1. 입력된 VOC 분석 대상 중복 제거 (PK를 기준으로 가장 첫번쨰 voc만 남겨두고, 중복되는 voc는 필터링)
    2. 입력된 VOC 분석 대상 필터링 (특수문자 & 자모 & 반복 & 10음절)
    3. NLP - 문항설문조사채널 / 고객경험단계구분과 관련있는 VOC인지 필터링
- 프롬프트:
    - `voc_filter_human`
    - `voc_filter_no_stage_system`
    - `voc_filter_stage_system`
- 입력:
    - voc_length_limit: VOC 필터링할 때 어느 음절 기준 밑으로 필터링할 것인지 Limit (기본 10)
    - batch_size: 배치 사이즈(batch_size)
    - voc_chunk_size: 하나의 LLM 요청에 몇개의 VOC를 입력 Context로 넘길 것인지 사이즈 (기본 50)
    - batch_sleep: 동시적으로 batch_size만큼 LLM을 호출한 뒤 얼마의 텀을 두고 다음 batch_size를 동시에 호출할 것인지 (단위: 초, 기본 1.5초)
    - ch_stge_dict: `VOC 수집 결과[2]`
    - voc_list: `VOC 수집 결과[0]`
- 출력:
    - 각각의 voc_info VO의 멤버변수
        - filtered_yn: 필터링 여부
        - filtered_reason: 필터링된 이유

### 4. 고객경험요소 매핑

- 채널 / 고객경험단계 별 고객경험요소를 Context로 VOC 원문에 연관된 고객경험요소를 매핑한다.
- 기능:
    1. Cxe Context 세팅 (고객경험단계 유/무) - '해당무'일 경우, 채널의 모든 고객경험요소를 Context로 활용
    2. LLM 실행 (결과는 복수 매칭 가능, 가장 연관성이 높은 고객경험요소 순서로 반환)
    3. 매핑 결과를 실제 고객경험요소와 비교하며, 최종 결과 세팅
- 프롬프트:
    - `cxe_mapping_human`
    - `cxe_mapping_system`
- 입력:
    - batch_size: 배치 사이즈(batch_size)
    - batch_sleep: 동시적으로 batch_size만큼 LLM을 호출한 뒤 얼마의 텀을 두고 다음 batch_size를 동시에 호출할 것인지 (단위: 초, 기본 1.5초)
    - ch_stge_cxe_dict: `VOC 수집 결과[1]`
    - voc_list: `VOC 수집 결과[0]`
- 출력:
    - 각각의 voc_info VO의 멤버변수
        - cxe_success_yn: 고객경험요소 매핑 성공 여부
        - cxe_failed_reason: 고객경험요소 매핑 실패 이유
        - cxe_result: 매핑된 고객경험요소 정보 LIST
            - seq: 정렬 순서, VOC와 가장 연관된 고객경험요소일 수록 낮음 1-Based
            - sq_cd, sq_nm: 고객경험요소에 묶여있는 서비스품질요소
            - cxe_cd, cxe_nm: 고객경험요소코드와 명

### 5. 감정 분류

- 고객경험요소 추출 유무에 따라 Context를 달리하며 VOC의 감정을 분석한다.
- 고객경험요소 추출 성공 VOC일 때, 대표 고객경험요소(정렬순서: 1)인 고객경험요소에 대해서만 실행
- 기능:
    1. Context 세팅 (고객경험요소 추출/미추출) 
        - 추출일 경우, VOC원문 내용 중에서도 대표 고객경험요소에 가장 적합한 내용을 포커스로 감정을 분석  
        - 미추출일 경우, VOC원문 내용 중에서도 채널, 고객경험단계를 포커스로 감정을 분석
    2. LLM 실행
    3. 분석 결과를 실제 감정과 비교하며, 최종 결과 세팅
- 프롬프트:
    - `emotion_analysis_cxe_human`
    - `emotion_analysis_cxe_system`
    - `emotion_analysis_no_cxe_human`
    - `emotion_analysis_no_cxe_system`
- 입력:
    - batch_size: 배치 사이즈(batch_size)
    - batch_sleep: 동시적으로 batch_size만큼 LLM을 호출한 뒤 얼마의 텀을 두고 다음 batch_size를 동시에 호출할 것인지 (단위: 초, 기본 1.5초)
    - ch_stge_cxe_dict: `VOC 수집 결과[1]`
    - emotion_dict: `VOC 수집 결과[3]`
    - voc_list: `VOC 수집 결과[0]`
- 출력:
    - 각각의 voc_info VO의 멤버변수
        - emtn_success_yn: 감정분석 성공 여부
        - emtn_failed_reason: 감정분석 실패 이유
        - emtn_result: 분석된 감정정보 정보
            - emtn_mid_cd, emtn_mid_nm: 감정 중분류
            - emtn_lag_cd, emtn_lag_nm: 감정 대분류
            - voc_typ_cd, voc_typ_nm: VOC 유형

### 6. 개체어 식별

- 고객경험요소 추출 유무에 따라 Context를 달리하며 개체어(상품서비스용어, 성능품질용어)를 추출한다.
- 고객경험요소 추출 성공 VOC일 때, 대표 고객경험요소(정렬순서: 1)인 고객경험요소에 대해서만 실행
- 기능:
    1. Context 세팅 (고객경험요소 추출/미추출) 
        - 추출일 경우, VOC원문 내용 중에서도 대표 고객경험요소에 가장 적합한 내용을 포커스로 개체어를 추출 
        - 미추출일 경우, VOC원문 내용 중에서도 채널, 고객경험단계를 포커스로 개체어를 추출
    2. LLM 실행
    3. 최종 결과 세팅
- 프롬프트:
    - `entity_word_detect_cxe_human`
    - `entity_word_detect_cxe_system`
    - `entity_word_detect_no_cxe_human`
    - `entity_word_detect_no_cxe_system`
- 입력:
    - batch_size: 배치 사이즈(batch_size)
    - batch_sleep: 동시적으로 batch_size만큼 LLM을 호출한 뒤 얼마의 텀을 두고 다음 batch_size를 동시에 호출할 것인지 (단위: 초, 기본 1.5초)
    - voc_chunk_size: 하나의 LLM 요청에 몇개의 VOC를 입력 Context로 넘길 것인지 사이즈 (기본 50)
    - prev_word_size: 하나의 LLM 요청에 들어가는 Chunk에 Context로 이전 개체어 실행결과를 각각 몇개씩 첨부할 것인지 사이즈(기본 15개 씩).
    - ch_stge_cxe_dict: `VOC 수집 결과[1]`
    - prev_prdct_svc_word_dict: `VOC 수집 결과[4]`
    - prev_pfrm_qalty_word_dict: `VOC 수집 결과[5]`
    - voc_list: `VOC 수집 결과[0]`
- 출력:
    - 각각의 voc_info VO의 멤버변수
        - cxe_result[0]: 이전 기능인 `고객경험요소 매핑`에서 세팅된 결과 객체의 대표
            - detect_success_yn: 개체어 식별 성공 여부
            - prdct_svc_word: 추출된 상품 서비스 용어
            - pfrm_qalty_word: 추출된 성능 품질 용어

### 7. 문제원인 식별

- 분류된 감정대분류가 '부정'인 VOC만을 대상으로, 고객경험요소 추출 유무에 따라 Context를 달리하여 문제원인을 식별한다.
- 고객경험요소 추출 성공 VOC일 때, 대표 고객경험요소(정렬순서: 1)인 고객경험요소에 대해서만 실행
- 기능:
    1. Context 세팅 (고객경험요소 추출/미추출) 
        - 추출일 경우, VOC원문 내용 중에서도 대표 고객경험요소에 가장 적합한 내용을 포커스로 문제원인을 식별  
        - 미추출일 경우, VOC원문 내용 중에서도 채널, 고객경험단계를 포커스로 문제원인을 식별
    2. LLM 실행
    3. 최종 결과 세팅
- 프롬프트:
    - `voc_problem_reason_human`
    - `voc_problem_reason_cxe_system`
    - `voc_problem_reason_no_cxe_system`
- 입력:
    - batch_size: 배치 사이즈(batch_size)
    - batch_sleep: 동시적으로 batch_size만큼 LLM을 호출한 뒤 얼마의 텀을 두고 다음 batch_size를 동시에 호출할 것인지 (단위: 초, 기본 1.5초)
    - ch_stge_cxe_dict: `VOC 수집 결과[1]`
    - ch_stge_dict: `VOC 수집 결과[2]`
    - voc_list: `VOC 수집 결과[0]` 중에서도 감정 대분류가 부정 ('02')인 VOC들만
- 출력:
    - 각각의 voc_info VO의 멤버변수
        - prblm_reason_success_yn: 문제원인 식별 성공여부
        - prblm_reason_result: 문제원인 결과

### 8. 응답

- 실행 한 사이클 동안 실행된 VOC 분석 결과와 페이징 정보, 그 외 메타 정보를 세팅해서 DTO로 응답한다.
- VO to DTO 변환 부분: `DataAnalysisAgent.convert_vo_to_dto()`
- 응답 DTO: `Z:\29_데이터\CX에이전트_배치 API 규격.xlsx 참고



## 3. 특징

1. 실행 Flow
    - 데이터 분석 에이전트의 최상위인, `DataAnalysisAgent` 클래스가 요청과 응답을 처리하고, 내부적으로 하위 클래스 `VocFilter`, `CxeMapper` 등등을 흐름대로 실행시키는 구조임.
2. LLM 호출 부 공통화
    - 모든 하위 Agent들의 LLM 객체 선언 부와 LLM 호출(invoke) 부를 `data_analysis/utils.py`의 `DataAnalysisUtils` Static 클래스로 공통화 해두었음.
    - LLM 객체 선언 부: get_llm()
    - LLM 호출 부: ainvoke_llm()
        - 현재 기본 LLM(`GPT-5`)로 호출 실패 시, Fail-bakc LLM(`GPT-5-mini`)로 호출 하도록 되어있음.
3. 실행에 필요한 공통 상수
    - 데이터 분석 실행 중 필요한 모든 상수 (키값, 메시지 등등)은 `data_analysis/model/consts.py`로 모아뒀음.
4. LLM 응답 결과에 따른 Parsing 전략
    - 현재, [VOC 필터], [고객경험요소 매핑], [문제원인 식별]은 LLM 응답이 XML 형태로 나오기 때문에 `DataAnalysisUtils.parse_llm_xml_to_dict()`로 응답 파싱함.
    - 나머지, [감정 분석], [개체어 식별]은 Json 형태기 때문에, `Langchain.jsonParser`를 사용하여 응답 파싱함.
5. 쿼리 파일 수정 시
    - 현재 쿼리를 실행하고, 결과값을 세팅하는 부분은 `VocGatherer`에 모두 몰아져있음.
    - 쿼리를 실행하고, 실행된 결과를 바탕으로 필요한 최종 결과값을 매핑하는 부분이 컬럼의 Alias로 되어있으니 만약 쿼리의 Alias가 변경되거나, 추가 제거되는 컬럼이 있다면, `VocGatherer`의 매핑 부분도 함께 수정해줘야함.
6. 고객경험요소 미매핑 시, 코드를 '9999'로 해둠
    - 추후 미매핑 코드가 수정되면, `model/consts.py`의 CommonCode.NO_CXE_ID 수정해야함.

## 999. TODO

1. 고객경험요소 기본 테이블이 현재 임시 마스터 테이블(sqlite)에서 조회해오고 있는데, 추후 mysql(inst1)에 테이블이 생기면 쿼리파일 (`resources/sql/select_cxe_standard.sql`) 수정하고, `VocGatherer.read_ch_stge_cxe_dict()`의 `sqlite_query` -> `mysql_query`로 수정

2. 이전 실행 결과 개체어 조회 또한 마찬가지로 임시 마스터 테이블(sqlite)에서 조회해오고 있는데, 추후 mysql(inst1)에 테이블이 생기면 쿼리파일 (`resources/sql/select_prev_entity_word.sql`) 수정하고, `VocGatherer.get_prev_word_result_set()`의 `sqlite_query` -> `mysql_query`로 수정

3. 실제로 NPS 시스템과 연동할 때는 일배치로 대상 년월일의 데이터 분석 결과를 output 파일로 적재하고, nps 시스템 배치에서 요청이 들어오면, 적재된 output 파일에서 단순히 데이터를 읽어들여 응답하는 형식이어야 함.
    1. 일배치로 호출하는 스케줄러 등록 개발 되어야 함.
    2. 호출된 결과를 OUTPUT 경로에 파일로 적재하는 기능이 개발 되어야 함.
    3. NPS와 연동되는 API(적재된 파일 읽어서 단순 반환)가 개발 되어야 함.

4. 현재는 MYSQL 조회할 때 LIMIT이 500인데, 추후 LIMIT이 변경된다면, `voc_gathering.py`의 MYSQL_QUERY_LIMIT 상수 변경
    - 지금은 PageSize가 MYSQL_QUERY_LIMIT 초과로 들어와도 내부적으로 MYSQL_QUERY_LIMIT 개씩 끊어서 PageSize 채울 때까지 반복하도록 해놓음.