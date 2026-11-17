#!/usr/bin/env python3
"""Collect graph state locally; optionally run the unchanged evaluator in another Python."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time

MAIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MAIN_DIR))
from evaluation.dataset import build_evaluation_case, executed_tool_trace, load_questions, validate_dataset, write_json


def load_checkpoint(path: Path, questions: list[dict]) -> dict[int, dict]:
    records = {}
    if not path.exists():
        return records
    lines = path.read_text(encoding="utf-8").splitlines()
    for number, line in enumerate(lines, 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            # A killed process may leave one partial final write. Repair before appending.
            if number == len(lines):
                path.write_text("\n".join(lines[:-1]) + ("\n" if lines[:-1] else ""), encoding="utf-8")
                break
            raise ValueError(f"체크포인트 {number}행이 손상되었습니다.")
        index = record["index"]
        if index >= len(questions) or record["question"] != questions[index]:
            raise ValueError("체크포인트와 질문 목록이 다릅니다. 새 출력 폴더를 사용하세요.")
        if record["status"] == "ok":
            validate_dataset([record["case"]])
            records[index] = record
    if path.stat().st_size and not path.read_bytes().endswith(b"\n"):
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n")
    return records


async def collect(agent, questions, output_dir, user_id, timeout, resume=False, context_setter=None):
    journal = output_dir / "traces.jsonl"
    records = load_checkpoint(journal, questions) if resume else {}
    dataset_path = output_dir / "cx_agent_eval_input.json"
    failed = []
    for index, question in enumerate(questions):
        if index in records:
            print(f"[{index + 1}/{len(questions)}] 재사용: {question['id']}", flush=True)
            continue
        started = time.monotonic()
        print(f"[{index + 1}/{len(questions)}] 실행: {question['id']}", flush=True)
        record = {"index": index, "question": question}
        if context_setter:
            context_setter(user_id)
        try:
            state = await asyncio.wait_for(agent.execute_eval(
                question["input"], user_id=user_id,
                conversation_history=[tuple(message) for message in question["conversation_history"]],
            ), timeout=timeout)
            case = build_evaluation_case(question, state)
            record.update(status="ok", case=case, state={
                key: state[key] for key in (
                    "intent", "intent_reason", "policy_violated", "policy_reason",
                    "tools_used", "queries_executed", "query_results", "evaluation_evidence",
                ) if key in state
            })
            record["executed_tools"] = executed_tool_trace(state)
            records[index] = record
        except Exception as exc:
            record.update(status="error", error=f"{type(exc).__name__}: {exc}")
            failed.append(index)
            print(f"  실패: {record['error']}", file=sys.stderr, flush=True)
        finally:
            if context_setter:
                context_setter(None)
        record["elapsed_seconds"] = round(time.monotonic() - started, 3)
        with journal.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        write_json(dataset_path, [records[i]["case"] for i in sorted(records)])
    # Also regenerate the export when every case was resumed.
    write_json(dataset_path, [records[i]["case"] for i in sorted(records)])
    write_json(output_dir / "collection_summary.json", {
        "total": len(questions), "collected": len(records), "failed": len(failed),
        "failed_ids": [questions[index]["id"] for index in failed],
        "dataset": str(dataset_path),
        "case_mapping": [
            {"test_case_id": f"TC-{export_index:04d}", "question_id": questions[index]["id"],
             "question_index": index}
            for export_index, index in enumerate(sorted(records))
        ],
    })
    print(f"수집: {len(records)}/{len(questions)}, 실패: {len(failed)} → {dataset_path}", flush=True)
    return dataset_path, len(failed)


async def collect_live(args, questions, output_dir):
    # Match the Docker working directory so .env and existing resource paths resolve correctly.
    app_dir = MAIN_DIR / "app"
    sys.path.insert(0, str(app_dir))
    os.chdir(app_dir)
    from core.config import settings
    from core.mcp_util import get_mcp_executor, set_user_id
    from agent import get_agent
    import mcp_sdk

    user_id = args.user_id or settings.MCP_USER_ID
    manifest = {"questions": questions, "user_id": user_id}
    manifest_path = output_dir / "collection_run.json"
    if args.resume:
        if not manifest_path.exists() or json.loads(manifest_path.read_text(encoding="utf-8")) != manifest:
            raise ValueError("재개할 실행의 질문/사용자 정보가 다릅니다. 새 출력 폴더를 사용하세요.")
    else:
        write_json(manifest_path, manifest)
    mcp_sdk.install()
    mcp_sdk.setup_system(client_id=settings.MCP_USER_ID, client_secret=settings.MCP_SECRET_KEY)
    set_user_id(user_id)
    try:
        executor = await get_mcp_executor()
        agent = get_agent("report", executor)
        return await collect(agent, questions, output_dir, user_id, args.timeout,
                             resume=args.resume, context_setter=set_user_id)
    finally:
        set_user_id(None)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--questions", type=Path, help="JSON/CSV/TSV/TXT 또는 testset 리스트가 있는 Python 파일")
    inputs.add_argument("--data", type=Path, help="이미 수집한 DeepEval JSON (--evaluate와 함께 사용)")
    parser.add_argument("--output-dir", type=Path, help="결과 폴더 (기본: evaluation/output/실행시각)")
    parser.add_argument("--user-id", help="MCP/LLM 호출 사용자 (기본: 환경의 MCP_USER_ID)")
    parser.add_argument("--limit", type=int, help="앞에서부터 실행할 질문 수")
    parser.add_argument("--timeout", type=float, default=300, help="질문별 수집 제한 시간(초)")
    parser.add_argument("--resume", action="store_true", help="같은 출력 폴더의 성공 건을 재사용하고 실패 건만 재시도")
    parser.add_argument("--evaluate", action="store_true", help="수집 후 기존 DeepEval 평가기를 실행")
    parser.add_argument("--evaluator-python", default=sys.executable, help="평가 전용 가상환경의 Python 경로")
    parser.add_argument("--deepeval-dir", type=Path, default=MAIN_DIR.parent / "deepeval")
    args = parser.parse_args(argv)
    if args.timeout <= 0 or (args.limit is not None and args.limit <= 0):
        parser.error("timeout/limit은 양수여야 합니다.")
    if args.data and (not args.evaluate or args.resume or args.limit is not None):
        parser.error("--data는 --evaluate와 사용하며 --resume/--limit과 함께 사용할 수 없습니다.")
    if args.resume and not args.output_dir:
        parser.error("--resume에는 --output-dir이 필요합니다.")
    # Resolve paths before collect_live changes cwd.
    output_dir = (args.output_dir or MAIN_DIR / "evaluation" / "output" /
                  datetime.now().strftime("%Y%m%d_%H%M%S_%f")).resolve()
    deepeval_dir = args.deepeval_dir.resolve()
    evaluator_python = str(Path(args.evaluator_python).resolve())
    failures = 0
    try:
        if output_dir.is_relative_to(deepeval_dir):
            raise ValueError("수집/평가 결과는 보호된 deepeval/ 폴더 밖에 저장해야 합니다.")
        if args.data:
            dataset_path = args.data.resolve()
            validate_dataset(json.loads(dataset_path.read_text(encoding="utf-8")))
        else:
            questions = load_questions((args.questions or MAIN_DIR / "evaluation" / "questions.example.json").resolve())
            if args.limit:
                questions = questions[:args.limit]
            if not args.resume and any((output_dir / name).exists() for name in
                                       ("traces.jsonl", "collection_run.json", "cx_agent_eval_input.json")):
                raise ValueError("기존 수집 결과가 있습니다. --resume 또는 새 출력 폴더를 사용하세요.")
            output_dir.mkdir(parents=True, exist_ok=True)
            dataset_path, failures = asyncio.run(collect_live(args, questions, output_dir))
        if args.evaluate:
            validate_dataset(json.loads(dataset_path.read_text(encoding="utf-8")))
            result = subprocess.run([
                evaluator_python, "-B", str(MAIN_DIR / "evaluation" / "evaluate.py"),
                "--data", str(dataset_path), "--deepeval-dir", str(deepeval_dir),
                "--output", str(output_dir / "results.json"),
            ], check=False)
            if result.returncode:
                return result.returncode
        return 1 if failures else 0
    except (OSError, ValueError, ImportError) as exc:
        print(f"실행 실패: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
