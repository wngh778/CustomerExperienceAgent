# Langfuse 없이 CX Agent 평가하기

`ReportGenerationAgent.execute_eval()`의 최종 LangGraph state에서 평가 데이터를 수집합니다.
`../../deepeval/`의 코드, 평가 모델, 지표, 임계값은 수정하지 않습니다.

## 준비

이 문서의 명령은 `main/`에서 실행합니다. 수집 명령의 `python`은 기존 에이전트를
실행하던 내부망 Python 환경을 사용하세요. `app/.env`, 내부 `python-mcp-sdk`,
LLM 및 MCP 접근 권한이 필요합니다. FastAPI 서버를 띄울 필요는 없습니다.

DeepEval 3.6.8은 Pydantic >=2.11.7을 요구하지만 서버 requirements는 2.10.6을
고정하고 있으므로 **평가 전용 환경을 따로 생성**합니다.

```bash
python3 -m venv .venv-eval
.venv-eval/bin/python -m pip install --find-links ../deepeval -r evaluation/requirements.txt
```

내부망에서는 기존 내부 패키지 인덱스를 사용하세요. `--find-links`는 제공된 wheel을
찾게 해주며, 함께 필요한 다른 의존성은 인덱스나 사내 wheel 저장소에서 설치해야 합니다.
평가용 키를 별도로 지정할 때는 기존 평가기가 지원하는 `AZURE_API_KEY` 환경 변수를
설정합니다. 평가 LLM의 endpoint와 기본 모델은 원래 평가기 설정을 그대로 사용합니다.

## 한 번에 수집하고 평가

```bash
python evaluation/run.py \
  --questions evaluation/questions.example.json \
  --output-dir evaluation/output/first-run \
  --evaluate \
  --evaluator-python .venv-eval/bin/python
```

기존 코드에 들어 있는 질문 목록도 실행할 수 있습니다. 먼저 일부만 확인하려면:

```bash
python evaluation/run.py \
  --questions app/tests/chat_test.py --limit 3 \
  --output-dir evaluation/output/smoke \
  --evaluate --evaluator-python .venv-eval/bin/python
```

`--limit`을 빼면 질문 전체를 실행합니다. Python 질문 파일은 `testset` 리터럴만
읽고 파일 자체는 실행하지 않습니다. JSON 문자열 리스트/객체 리스트,
CSV/TSV(`input`, `query`, `text`, `질문` 중 하나), 한 줄에 질문 하나인 TXT도 지원합니다.
같은 질문이 여러 번 있어도 각각 실행합니다.

`--user-id` 기본값은 환경의 `MCP_USER_ID`입니다. 수집은 순차 실행하며,
질문당 제한 시간은 `--timeout 300`으로 조정할 수 있습니다.
평가 실행은 원래 평가기의 동시성/타임아웃 로직을 그대로 따릅니다.

## 수집과 평가를 따로 실행

```bash
# 수집만: 에이전트 실행용 Python
python evaluation/run.py --output-dir evaluation/output/first-run

# 평가만: 기존 수집 데이터를 재사용 (에이전트/MCP 의존성 불필요)
.venv-eval/bin/python evaluation/evaluate.py \
  --data evaluation/output/first-run/cx_agent_eval_input.json \
  --output evaluation/output/first-run/results.json

# 중단/실패한 수집 재개: 같은 질문 목록과 사용자 사용
python evaluation/run.py \
  --output-dir evaluation/output/first-run --resume \
  --evaluate --evaluator-python .venv-eval/bin/python
```

재개 시 성공 건은 다시 호출하지 않고 실패 건을 재시도합니다. `--questions`, `--limit`,
`--user-id`를 사용한 실행은 재개할 때도 같은 값을 전달하세요.
기존 결과 폴더에 새 수집을 덮어쓰려 하면 오류를 반환합니다.

## 추출 기준과 결과

평가기의 실제 입력 키는 `actual_output`이 아니라 **`response`**입니다.

```json
[
  {
    "input": "사용자 질문",
    "response": "최종 답변",
    "tool": ["실제로 실행된 도구 이름"],
    "context": ["답변에 사용한 조회 결과 또는 매뉴얼"]
  }
]
```

- SQL 분석: 실행 완료된 `ToolMessage` 중 `query_results`에 반영된 결과를 수집합니다.
  답변 모델과 같은 포맷터를 사용해 결과당 최대 50행과 동일한 반올림/컬럼 압축을 적용합니다.
  실행되지 않은 다음 도구 계획이나 `tool_skip`을 평가 근거로 넣지 않습니다.
- 매뉴얼: 실제 답변 작성 시 읽은 매뉴얼과 고객경험 체계 텍스트를 저장합니다.
- 일반 대화/정책 거부: 조회 근거가 없으면 `tool`, `context`는 빈 리스트입니다.
  시스템 프롬프트나 최종 답변을 근거로 채우지 않습니다.
- JSON 질문 객체에 `conversation_history`를 넣으면 에이전트에도 전달합니다.
  평가기의 `input`에는 이전 대화와 현재 질문을 함께 넣어 후속 질문을 평가할 수 있게 합니다.

출력 폴더에 다음 파일이 생깁니다.

| 파일 | 내용 |
| --- | --- |
| `cx_agent_eval_input.json` | 원래 평가기에 넘기는 성공 건의 JSON 리스트 |
| `traces.jsonl` | 질문별 상태, 실행된 도구의 인자/원본 출력, 실패, 실행 시간 |
| `collection_run.json` | 재개 시 확인하는 질문/사용자 정보 |
| `collection_summary.json` | 수집 성공/실패 수, 원래 질문 ID와 DeepEval TC-ID 대응 |
| `results.json` | 원래 DeepEval 로직으로 계산한 지표와 점수 |
| `results.log` | 평가 로그 |

수집 실패 건은 빈 답변으로 평가하지 않습니다. 성공 건을 평가한 뒤에도 수집 실패나
지표 실행 오류가 있으면 종료 코드 1을 반환합니다. 점수 기준 미달은 평가 실행 오류와 구분합니다.

원래 평가기에서 활성화된 지표는 Answer Relevancy, Toxicity, Text Fluency이고,
근거가 있는 경우 Faithfulness가 추가됩니다. **ToolUseMetric, Contextual Relevancy,
Hallucination은 원래 코드에서 비활성화되어 있으므로 점수를 생성하지 않습니다.**
`tool`은 추적/호환 목적으로 보존합니다.

평가 어댑터는 원래 모듈을 읽어 `cx`의 데이터 경로만 메모리에서 지정합니다.
bytecode 생성과 DeepEval telemetry를 끄고, 캐시/로그는 결과 폴더에 저장합니다.
실행 전후 `deepeval/` 전체 파일의 SHA-256을 비교하며 해당 폴더 안으로 결과를 쓰는 경로는 거부합니다.
`monthly.py`는 별도의 과거 월별 집계 스크립트이므로 이 실행 경로에 포함하지 않습니다.

## 로컬 검증

```bash
python -B -m unittest discover -s evaluation/tests -v
```

기본 테스트는 외부 패키지/네트워크 없이 실행됩니다. LangGraph/DeepEval이 설치되어 있으면
실제 그래프 및 원래 평가기와의 통합 테스트도 실행합니다. 통합 테스트의 LLM/도구는
고정 응답을 사용하므로, 그 테스트 결과는 실제 에이전트의 품질 점수가 아닙니다.
