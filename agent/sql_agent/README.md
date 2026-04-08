# SQL Agent

## 사용 방법
1. ./resources/metadata 경로에 사용할 {table_name}.yaml을 작성 (gpt-oss-120b 모델과, kb메타 활용)
2. WP-KB0-00107-TA-0001/app 경로에서 uvicorn api.main:app --port {포트} 실행
3. WP-KB0-00107-TA-0001/app/tests 경로에서 agent_call_test.py 파일 수정
  - agent_type을 "sql"로 수정
  - input_json의 content에 질문 내용 수정
  - request.post url에 api띄운 port로 수정
4. agent_call_test.py 실행

## 메타데이터 파일 형식

각 테이블마다 하나의 YAML 파일을 생성합니다. 파일명은 `{테이블명}.yaml`입니다.

예시 (`metadata/users.yaml`):

```yaml
table_name: users
description: 사용자 정보를 저장하는 테이블
columns:
  - name: id
    type: integer
    description: 사용자 고유 ID
    nullable: false
  - name: name
    type: string
    description: 사용자 이름
    nullable: false
  - name: email
    type: string
    description: 사용자 이메일 주소
    nullable: false
  - name: age
    type: integer
    description: 사용자 나이
    nullable: true
```

## SQL Agent 절차
1. 사용 가능한 table 목록 조회 (metadata 폴더 내 있는 table들): list_tables_tool
2. 질문에 따라 사용할 table을 고르고, 해당 table에 대한 schema 정보 조회: schema_tool
3. 사용할 table의 5개 sample 데이터 조회: sample_tool
4. 쿼리 작성 후 실행: query_tool
5. 쿼리 오류 발생 여부 확인
6. 오류 발생시 쿼리 재작성 후 실행
7. 응답

## 주의사항
- 테스트 중 max iteration 혹은 token error 발생하면 api 재실행
- 루프가 너무 많이돈다면 프롬프트에 뭔가 문제 있을 확률이 높음