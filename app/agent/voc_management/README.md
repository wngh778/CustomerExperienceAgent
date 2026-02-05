# voc_management_agent

이 프로젝트는 VOC(Voice of Customer) 데이터를 기반으로 키워드 추출, CX 요소 매칭, 관련성 판단, 개선 제안 및 요약 리포트를 생성하는 파이프라인입니다. 아래의 흐름에 따라 각 모듈이 순차적으로 실행됩니다.

## 프로젝트 구조
```
app
  └── agent
      └── voc_management
          ├── resources
          │   ├── prompt
          │   │   ├── judge_template.txt
          │   │   ├── keywords_extract_no_rag.txt
          │   │   ├── relevance_check.txt
          │   │   ├── suggestion_generate.txt
          │   │   ├── voc_summary.txt
          │   │   └── feedback_generate.txt (voc_feedback)
          │   ├── query
          │   │   └── match_keywords_cxElmntCtnt.sql
          │   └── template
          │       └── feedback_template.txt (voc_feedback)
          ├── tools
          │   ├── voc_simalloc_opt
          │   │   ├── KeywordCxMatcher.py
          │   │   ├── KeywordExtractionPipeline.py
          │   │   ├── RelevanceMatcher.py
          │   │   └── SuggestionGenerator.py
          │   └── voc_feedback
          │       ├── build_feedback_message.py
          │       ├── create_feedback_messages.py
          │       └── generate_llm_feedback.py
          └── utils
              ├── load_files.py
              └── text_preprocessing.py
```

## Flow 차트
- Request -> KeywordExtractionPipeline -> KeywordCxMatcher -> RelevanceMatcher -> SuggestionGenerator

---

## 개선의션 작성 모듈 설명 및 참조 프롬프트

### KeywordExtractionPipeline
- 기능: DataFrame의 각 VOC 텍스트에서 키워드를 비동기로 추출해 keywords 컬럼으로 반환하는 파이프라인
- 참조 프롬프트: `resources/prompt/keywords_extract_no_rag.txt`

### KeywordCxMatcher
- 기능: 키워드와 CX 요소를 기반으로 MySQL에서 관련 응답·검토 데이터를 비동기로 조회해 매칭 결과 DataFrame을 생성합니다.
- 참조 쿼리: `resources/query/match_keywords_cxElmntCtnt.sql` -> 해당 쿼리는 추후 고객경험요소가 완성되면 변경이 필요합니다. 현재 서비스품질요소코드를 활용중

### RelevanceMatcher
- 기능: 주어진 VOC와 후보 응답 목록을 LLM으로 판단해 관련 응답의 인덱스를 추출하고, 그룹 단위 비동기 처리로 매칭된 행만 정제하는 기능
- 참조 프롬프트: `resources/prompt/relevance_check.txt`

### SuggestionGenerator
- 기능: VOC 데이터(고객별 의견/코드/프로젝트 등)를 묶어 LLM으로 개선의견·검토구분을 제안하고 요약 리포트를 생성하는 클래스
- 참조 프롬프트: 
  - `resources/prompt/judge_template.txt`
  - `resources/prompt/suggestion_generate.txt`
  - `resources/prompt/voc_summary.txt`

---

## 피드백 작성 에이전트

### build_feedback_message
- 기능: 고객 VOC 데이터를 바탕으로 템플릿을 로드하여 고객 ID, CX/CXC 정보, LLM 피드백을 채워 완성된 피드백 메시지를 생성합니다. 데이터가 비어있는 경우 기본값(예: 고객, 빈 문자열)을 사용해 안정적으로 메시지를 구성합니다.

- 참조 프롬프트:
- 입력: data(Dict[str, str])와 llm_feedback(str)
- 처리:
  - load_template("feedback_template.txt")로 템플릿 로드
  - data에서 qusnInvlTagtpUniqID, cx, cxc 추출 및 공백 제거
  - 기본값 적용: customer_id가 비면 "고객", cx/cxc가 비면 ""
  - llm_feedback 공백 제거 후 템플릿에 치환
- 출력: 완성된 피드백 메시지 문자열

### generate_llm_feedback
- 기능: 제안 텍스트를 입력받아 LLM을 통해 피드백을 생성하는 비동기 함수. 사전 정의된 프롬프트 템플릿을 로드해 SystemMessage로 구성하고, Azure Chat OpenAI 모델에 요청하여 결과를 반환함. 응답 객체 유형에 따라 content 추출 로직을 포함해 안전하게 문자열로 변환.

- 참조 프롬프트: feedback_generate.txt (load_prompt로 로드 후 suggestionText 변수를 채워 사용)

### create_feedback_messages
- 기능:  
    - 배치로 들어온 데이터 리스트를 순회하며 각 항목의 제안 텍스트를 추출합니다.  
    - generate_llm_feedback 함수를 통해 LLM 피드백을 생성합니다.  
    - build_feedback_message로 피드백 메시지 콘텐츠를 구성합니다.  
    - 결과로 타임스탬프(ts), 질문 ID(qusnid), 태그 유니크 ID(qusnInvlTagtpUniqID), 피드백 콘텐츠(feedbankContent)를 담은 리스       트를 반환합니다.  
    - KST(UTC+9) 기준의 타임스탬프 형식은 "YYYYMMDD HH:MM:SS"입니다.  
    - 데이터 안전성: voc가 없거나 필드가 비어도 기본값("")로 처리하여 에러를 방지합니다.

- 참조 프롬프트:  
    - suggestion_text은 data.voc.suggestionText → data.suggestionText 순으로 우선 추출  
    - qusnInvlTagtpUniqID는 data.voc.qusnInvlTagtpUniqID → data.qusnInvlTagtpUniqID 순으로 우선 추출  
    - cx, cxc는 data.cx, data.cxc에서 추출  
    - generate_llm_feedback(suggestion_text) 호출로 LLM 결과 생성  
    - build_feedback_message({ qusnInvlTagtpUniqID, cx, cxc }, llm_feedback)로 최종 메시지 생성  
    - 반환 형태: { ts, qusnid, qusnInvlTagtpUniqID, feedbankContent } 리스트


