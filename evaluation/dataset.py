"""Dependency-free input parsing and the existing evaluator's JSON contract."""

from __future__ import annotations

import ast
import csv
import json
from pathlib import Path
from typing import Any


def load_questions(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        rows = json.loads(path.read_text(encoding="utf-8-sig"))
    elif suffix in {".csv", ".tsv"}:
        with path.open(encoding="utf-8-sig", newline="") as source:
            rows = list(csv.DictReader(source, delimiter="\t" if suffix == ".tsv" else ","))
    elif suffix == ".py":
        # The supplied app/tests/chat_test.py contains a literal testset. Never execute it.
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        assignments = [
            node.value for node in tree.body if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "testset" for target in node.targets)
        ]
        if len(assignments) != 1:
            raise ValueError("Python 질문 파일에는 리터럴 testset 리스트가 하나 있어야 합니다.")
        rows = ast.literal_eval(assignments[0])
    else:
        rows = [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]

    if not isinstance(rows, list) or not rows:
        raise ValueError("질문 파일은 비어 있지 않은 리스트여야 합니다.")
    questions = []
    for index, row in enumerate(rows):
        if isinstance(row, str):
            row = {"input": row}
        if not isinstance(row, dict):
            raise ValueError(f"질문 {index + 1}: 문자열 또는 객체가 필요합니다.")
        query = next((row[key] for key in ("input", "query", "text", "질문") if row.get(key)), None)
        if not isinstance(query, str) or not query.strip():
            raise ValueError(f"질문 {index + 1}: input/query/text/질문 컬럼이 필요합니다.")
        history = row.get("conversation_history") or []
        if not isinstance(history, list):
            raise ValueError(f"질문 {index + 1}: conversation_history는 리스트여야 합니다.")
        normalized_history = []
        for message in history:
            if isinstance(message, dict):
                role, content = message.get("role"), message.get("content")
            elif isinstance(message, (list, tuple)) and len(message) == 2:
                role, content = message
            else:
                raise ValueError(f"질문 {index + 1}: 대화 이력 형식이 잘못되었습니다.")
            if role not in {"user", "assistant"} or not isinstance(content, str):
                raise ValueError(f"질문 {index + 1}: 대화 이력에는 user/assistant 문자열만 허용됩니다.")
            normalized_history.append([role, content])
        questions.append({
            "id": str(row.get("id", row.get("ID", index + 1))),
            "input": query,
            "conversation_history": normalized_history,
        })
    return questions


def build_evaluation_case(question: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    """Export only final output and evidence captured at answer generation time."""
    evidence = state.get("evaluation_evidence", [])
    if state.get("tools_used") and "evaluation_evidence" not in state:
        raise ValueError("도구 사용 결과의 evaluation_evidence가 없습니다. 수정된 에이전트인지 확인하세요.")
    evaluation_input = question["input"]
    if question.get("conversation_history"):
        history = "\n".join(f"{role}: {content}" for role, content in question["conversation_history"])
        evaluation_input = f"[이전 대화]\n{history}\n\n[현재 질문]\n{evaluation_input}"
    case = {
        "input": evaluation_input,
        "response": state.get("final_answer"),
        "tool": [item["tool"] for item in evidence],
        "context": [item["context"] for item in evidence],
    }
    validate_dataset([case])
    return case


def executed_tool_trace(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Keep executed calls (including failures/skips) for audit, without system prompts."""
    calls = {}
    executions = []
    for message in state.get("messages", []):
        for call in getattr(message, "tool_calls", []) or []:
            calls[call.get("id")] = call
        if getattr(message, "type", None) != "tool":
            continue
        call_id = getattr(message, "tool_call_id", None)
        call = calls.get(call_id, {})
        executions.append({
            "tool_call_id": call_id,
            "name": getattr(message, "name", None) or call.get("name"),
            "args": call.get("args", {}),
            "output": getattr(message, "content", None),
            "status": getattr(message, "status", "success"),
        })
    return executions


def validate_dataset(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, list) or not data:
        raise ValueError("평가 데이터는 비어 있지 않은 JSON 리스트여야 합니다.")
    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"평가 데이터 {index + 1}: 객체가 필요합니다.")
        for field in ("input", "response"):
            if not isinstance(item.get(field), str) or not item[field].strip():
                raise ValueError(f"평가 데이터 {index + 1}: 비어 있지 않은 {field} 문자열이 필요합니다.")
        tools, contexts = item.get("tool", []), item.get("context", [])
        if not isinstance(tools, list) or not isinstance(contexts, list):
            raise ValueError(f"평가 데이터 {index + 1}: tool/context는 리스트여야 합니다.")
        if len(tools) != len(contexts):
            raise ValueError(f"평가 데이터 {index + 1}: tool/context 개수가 다릅니다.")
        if any(not isinstance(value, str) or not value.strip() for value in tools + contexts):
            raise ValueError(f"평가 데이터 {index + 1}: tool/context에는 비어 있지 않은 문자열이 필요합니다.")
    return data


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)
