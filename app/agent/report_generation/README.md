# CXM NPS 분석 에이전트 (v2)

KB국민은행 고객경험관리(CXM) NPS 분석 에이전트. 자연어 질의를 받아 SQL을 생성·실행하고 마크다운 분석 보고서를 반환하는 **LangGraph 기반 멀티노드 에이전트 시스템**.

## 주요 특징

- **자연어 → SQL → 분석 보고서** 전 과정 자동화 (End-to-End)
- **LangGraph 9-노드 파이프라인**: 의도 분류, 정책 가드, 질의 정제, 쿼리 플래닝, SQL 생성·실행, 분석 보고서 생성
- **Pydantic Structured Output**: 모든 의사결정 노드에서 타입 안전한 출력
- **24개 뷰 카탈로그**: TD(타행비교) / BU(실경험) 조사 체계를 포괄하는 스키마 레지스트리
- **정책 가드**: 노조 협약에 따른 개별 직원/영업점/지역본부 단위 NPS 조회 차단
- **병렬 SQL 실행**: `asyncio.gather`로 다중 쿼리 동시 생성·실행
- **GPT-5 최적화**: reasoning effort 차등 적용, XML 태그 기반 프롬프트

---

## 아키텍처

### LangGraph Flow

```
START → intent_classifier
         ├─ unsafe         → unsafe_responder → END
         ├─ nps_analysis   → policy_guard ─┬─ pass    → query_rewriter → query_planner → sql_executor → nps_analyst → END
         │                                 └─ blocked → policy_violation_responder → END
         ├─ manual         → manual_qa → END
         └─ general_chat   → general_responder → END
```

### 노드별 역할

| 노드 | 역할 | Structured Output |
|------|------|-------------------|
| `intent_classifier` | 4가지 의도 분류 (unsafe / nps_analysis / manual / general_chat) | `IntentClassification` |
| `policy_guard` | 개별 직원/영업점/본부 조회 정책 위반 여부 판단 | `PolicyGuardResult` |
| `query_rewriter` | CX 계층 참조하여 모호한 질의를 완전한 분석 질의로 정제 | `QueryRewrite` |
| `query_planner` | 24개 뷰 카탈로그에서 적합한 뷰 1~3개 선택 + 분석 전략 수립 | `QueryPlan` |
| `sql_executor` | SQL 생성 → 별칭 치환 → 전처리 → MCP 실행 (병렬) | `CompletedQuery` |
| `nps_analyst` | 조회 결과 기반 마크다운 분석 보고서 생성 | 마크다운 텍스트 |
| `unsafe_responder` | 유해 요청 고정 거부 응답 | - |
| `policy_violation_responder` | 정책 위반 시 거부 + 허용 범위 안내 | - |
| `manual_qa` | CXM 시스템 매뉴얼 기반 Q&A | 텍스트 |
| `general_responder` | 범위 밖 질문 안내 | 텍스트 |

### SQL 실행 파이프라인

```
LLM이 뷰 별칭(v_xxx)으로 SQL 작성
  → catalog.py:translate_sql()  →  실제 테이블명(INST1.TSCCVMG*)으로 치환
  → _rewrite_sql()              →  잔여 별칭 치환 + 무효 컬럼 조건 제거 + LIMIT 보장
  → mcp_executor                →  SQLite(테스트) 또는 MCP 서버(프로덕션)로 실행
```

---

## 프로젝트 구조

```
├── README.md                                # 이 파일
├── core/
│   ├── __init__.py
│   └── config.py                            # pydantic-settings 기반 설정
└── app/
    ├── __init__.py
    ├── main.py                              # 에이전트 단독 테스트 엔트리포인트
    └── agent/
        ├── __init__.py
        ├── agent_template.py                # Agent ABC (LLM, 로거 초기화)
        └── report_generation/               # ★ 메인 에이전트
            ├── __init__.py
            ├── report_generation_agent.py   # ReportGenerationAgent (LangGraph 전 노드 구현)
            └── resources/                   # 에이전트 리소스 (SSOT)
                ├── schema.py                # VIEW_REGISTRY: 24개 뷰 DDL/SQL 템플릿/카탈로그
                ├── catalog.py               # 뷰 별칭(v_xxx) ↔ 테이블명(INST1.*) 매핑
                ├── models.py                # Pydantic structured output 모델
                ├── prompts.py               # 노드별 시스템 프롬프트 (XML 태그 구조)
                ├── cx_hierarchy.txt         # CX 계층 TSV 데이터
                └── manual.md                # CXM 시스템 운영 매뉴얼
```

---


## 데이터 모델

### 조사 체계

| 구분 | TD (Top-Down) | BU (Bottom-Up) |
|------|---------------|-----------------|
| **주기** | 반기 1회 | 매일 |
| **대상** | 7개 은행 고객 (타행 비교) | KB 채널 이용 고객 |
| **시간 키** | `조사년도` + `반기구분명` | `기준년월일` (일별 누적) / `기준년월` (월말 스냅샷) |
| **채널** | 은행, 브랜드, 플랫폼, 대면채널, 고객센터, 상품 | 영업점, KB스타뱅킹, 고객센터, 상품 |
| **고유 분석** | IPA 4사분면, 타행 벤치마크 | 일별 추이, CCI/NSS, 영업점/직원 성과 |

### CX 계층 구조

```
채널 → 고객경험단계 → 서비스품질요소 → 고객경험요소
```

### 핵심 지표

| 지표 | 정의 |
|------|------|
| **NPS** | 추천비율(9~10점) − 비추천비율(0~6점) |
| **영향도** | (경험단계 추천자수 − 비추천자수) / 채널 전체 응답자수 × 100 |
| **NSS** | 긍정 VoC 비율 − 부정 VoC 비율 |
| **CCI** | 불만 VoC 건수 / 전체 VoC 건수 × 100 |

### 뷰 카탈로그 (24개)

| 카테고리 | 뷰 ID | 설명 |
|----------|--------|------|
| **TD NPS** | `v_td_channel_nps` | 채널별 NPS (타행 비교) |
| | `v_td_cx_stage_nps` | 경험단계별 NPS |
| **TD 영향도** | `v_td_channel_driver` | 채널 영향요인 (경험단계 영향도) |
| | `v_td_cx_stage_driver` | 경험단계 영향요인 (서비스품질요소 영향도) |
| **TD IPA** | `v_td_channel_ipa` | 채널 IPA 4사분면 |
| | `v_td_cx_stage_ipa` | 경험단계 IPA |
| **BU NPS** | `v_bu_channel_nps` | 채널별 NPS 누적 |
| | `v_bu_cx_stage_nps` | 경험단계별 NPS 누적 |
| | `v_bu_channel_nps_trend` | 채널 NPS 일별 추이 |
| **BU 영향도** | `v_bu_stage_factor` | 서비스품질요소별 영향도 |
| **BU 감성** | `v_bu_sentiment` | 감성분석 (NSS) |
| | `v_bu_voc_type` | VoC 유형별 집계 (CCI) |
| **VOC** | `v_voc_raw` | VOC 원문 조회 |
| | `v_voc_by_factor` | 서비스품질요소별 VoC 건수 |
| | `v_voc_cx_element` | 고객경험요소 빈도 |
| **스펙트럼** | `v_spectrum_age` | 연령대별 NPS |
| | `v_spectrum_gender` | 성별 NPS (BU) |
| | `v_spectrum_customer_type` | 고객유형별 NPS |
| | `v_spectrum_branch` | 영업점별 NPS (BU) | -> 사실상 정책적으로 막힘
| | `v_spectrum_employee` | 직원별 NPS (BU) | -> 사실상 정책적으로 막힘
| **개선조치** | `v_improvement_status` | 과제 상태별 현황 |
| | `v_improvement_by_dept` | 부서별 배분/처리율 |
| | `v_improvement_judgement` | 검토결과별 현황 |
| | `v_improvement_overdue` | 기한초과 건 목록 |

---

## 실행 예시

다음은 `"개선부서별 처리율 현황이랑 미처리 건 중 불만 VOC 내용 보여줘"` 질의의 실제 실행 로그이다.

### 1. Intent Classification

```
Intent: nps_analysis
Reason: 개선조치(미처리) 처리율 현황과 불만 VoC 원문 조회를 요청하는 것으로
        CXM의 NPS/VoC 분석 및 조치관리 영역에 해당함.
```

### 2. Policy Guard

```
Violated: False
Reason: 특정 직원/영업점/본부의 NPS를 조회하려는 요청이 아니며,
        개선부서별 처리율 및 미처리 VOC 내용은 개인/지점 식별 NPS 금지 범위에 해당하지 않습니다.
```

### 3. Query Rewrite

원문 질의를 BU 서술형 VOC 기반의 구체적 분석 질의로 정제:

```
원문: '개선부서별 처리율 현황이랑 미처리 건 중 불만 VOC 내용 보여줘'
정제: 'BU 서술형 VOC 데이터(v_bu_master_voc) 기준으로, 채널명 전체에서
      기준년월일=20251228 누적 기준의 개선부서명별 VOC 처리율 현황을 집계하고,
      미처리이면서 불만이고 VOC필터링여부=0인 건들의 VOC원문내용을 조회...'
```

### 4. Query Plan

```
Views: ['improvement_by_dept', 'bu_voc_raw']
Strategy: E.VOC인사이트
```

### 5. SQL Generation & Execution

2개의 SQL이 병렬로 생성·실행:

**쿼리 1** — 개선부서별 처리율 집계:
```sql
SELECT 개선부서명, COUNT(*) AS 배분건수,
       SUM(CASE WHEN 과제진행상태명 = '승인완료' THEN 1 ELSE 0 END) AS 승인완료건수,
       ROUND(SUM(CASE WHEN 과제진행상태명 = '승인완료' THEN 1 ELSE 0 END) * 100.0
             / NULLIF(COUNT(*), 0), 2) AS 처리율
FROM v_bu_master_voc
WHERE 기준년월일 = '20251228' AND 배분여부 = 1 AND VOC필터링여부 = 0
GROUP BY 개선부서명
```

**쿼리 2** — 미처리 불만 VOC 원문 조회:
```sql
SELECT 개선부서명, VOC원문내용
FROM v_bu_voc_raw
WHERE VOC필터링여부 = 0 AND 고객경험VOC유형명 = '불만' AND 과제진행상태명 = '미처리'
```

### 6. Analysis Report

`nps_analyst` 노드가 조회 결과를 기반으로 요약, 주요 포인트, 상세 분석, 특이사항을 포함한 마크다운 보고서를 생성.

---

## 보안 / 정책

| 분기 | 트리거 | 응답 |
|------|--------|------|
| `unsafe` | 비윤리적·불법적 요청, 개인정보 유출, 프롬프트 인젝션 | 고정 거부 메시지 |
| `policy_violation` | 개별 직원/영업점/지역본부 단위 NPS 조회 (노조 협약) | 정책 거부 + 허용 범위 안내 |

**정책 가드 판단 예시:**
- "영업점 채널 NPS 현황" → **허용** (채널 단위 집계)
- "강남지점 NPS" → **차단** (개별 영업점 식별)
- "영업점별 NPS 분포" → **차단** (영업점 단위 NPS 계산을 전제하므로 개별 영업점 성과 간접 노출)
- "김철수 직원 성과" → **차단** (개별 직원 식별)

---

## 컨벤션

- 프롬프트는 **XML 태그 구조** (`<role>`, `<rules>`, `<output_format>` 등)
- **한국어 컬럼명** 사용 (DB 스키마와 일치)
- VOC 집계 시 `VOC필터링여부=0` 조건 필수
- 개선조치 집계 시 `배분여부=1` 추가 필수
- 스펙트럼(세그먼트) 분석은 마스터 뷰에서 `CASE WHEN`으로 NPS/NSS/CCI 직접 계산
- 스키마 변경 시 `resources/schema.py`(VIEW_REGISTRY)만 수정 → **Single Source of Truth**

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| **에이전트 프레임워크** | LangGraph + LangChain |
| **LLM** | GPT-5 시리즈 (Azure OpenAI) |
| **설정 관리** | pydantic-settings (`.env` 기반) |
| **Structured Output** | Pydantic BaseModel + `with_structured_output()` |
| **DB (프로덕션)** | MCP 서버 경유 RDBMS |
| **관측성** | Langfuse (선택적) |
| **비동기** | asyncio (`gather`로 병렬 SQL 실행) |
