#!/usr/bin/env python3
"""Read-only adapter for ../deepeval/evaluate_agent_tool.py (no metric changes)."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

MAIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MAIN_DIR))
from evaluation.dataset import validate_dataset, write_json


def fingerprint(directory: Path) -> dict[str, str]:
    return {str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*") if path.is_file()}


def evaluate_existing(data_path: Path, deepeval_dir: Path, output_path: Path):
    data_path, deepeval_dir, output_path = data_path.resolve(), deepeval_dir.resolve(), output_path.resolve()
    if output_path.is_relative_to(deepeval_dir):
        raise ValueError("평가 결과는 보호된 deepeval/ 폴더 밖에 저장해야 합니다.")
    if data_path in {output_path, output_path.with_suffix(".log")}:
        raise ValueError("평가 결과/로그 경로는 입력 데이터 경로와 달라야 합니다.")
    validate_dataset(json.loads(data_path.read_text(encoding="utf-8")))
    source = deepeval_dir / "evaluate_agent_tool.py"
    if not source.is_file():
        raise FileNotFoundError(f"평가기 파일이 없습니다: {source}")
    before = fingerprint(deepeval_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    (output_path.parent / ".deepeval").mkdir(exist_ok=True)
    # DeepEval caches/logs belong to the output directory, not the supplied source directory.
    original_cwd, original_path, original_bytecode = Path.cwd(), sys.path[:], sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    os.environ["DEEPEVAL_TELEMETRY_OPT_OUT"] = "1"
    sys.path.insert(0, str(deepeval_dir))
    os.chdir(output_path.parent)
    try:
        spec = importlib.util.spec_from_file_location("cx_original_evaluator", source)
        evaluator = importlib.util.module_from_spec(spec)
        # Dataclass/model internals can resolve this module by name during import.
        sys.modules[spec.name] = evaluator
        spec.loader.exec_module(evaluator)
        # Only the input location changes; all models, metrics, thresholds and execution stay original.
        evaluator.AGENT_CONFIGS["cx"]["data_path"] = str(data_path)
        evaluator.logger = evaluator.setup_logging(str(output_path.with_suffix(".log")))
        result = evaluator.run_evaluation(agent_name="cx")
        payload = result.model_dump(mode="json")
        write_json(output_path, payload)
        errors = sum(bool(metric.get("error")) for case in payload["test_results"] for metric in case["metrics"])
        print(f"평가 완료: {payload['total_test_cases']}건, 통과율 {payload['overall_pass_rate']:.1%}, "
              f"지표 오류 {errors}건 → {output_path}", flush=True)
        return 1 if errors else 0
    finally:
        os.chdir(original_cwd)
        sys.path[:] = original_path
        sys.dont_write_bytecode = original_bytecode
        if fingerprint(deepeval_dir) != before:
            raise RuntimeError("보호된 deepeval/ 파일이 변경되었습니다.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True, type=Path)
    parser.add_argument("--deepeval-dir", default=MAIN_DIR.parent / "deepeval", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        return evaluate_existing(args.data, args.deepeval_dir, args.output)
    except Exception as exc:
        print(f"DeepEval 실행 실패: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
