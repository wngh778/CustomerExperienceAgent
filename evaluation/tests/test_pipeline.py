from __future__ import annotations

import asyncio
import ast
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from evaluation.dataset import build_evaluation_case, executed_tool_trace, load_questions, validate_dataset, write_json
from evaluation.run import collect, load_checkpoint
from evaluation.evaluate import evaluate_existing, fingerprint


class DatasetTests(unittest.TestCase):
    def test_supplied_python_testset_is_loaded_without_execution(self):
        path = Path(__file__).resolve().parents[2] / "app" / "tests" / "chat_test.py"
        rows = load_questions(path)
        self.assertGreater(len(rows), 50)
        self.assertEqual(rows[0]["input"], "NPS가 뭐야?")

    def test_python_input_never_executes_code(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "questions.py"
            marker = Path(directory) / "executed"
            path.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\ntestset = ['질문']\n")
            self.assertEqual(load_questions(path)[0]["input"], "질문")
            self.assertFalse(marker.exists())

    def test_csv_bom_and_duplicate_questions_keep_identity(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "questions.csv"
            path.write_text("\ufeffid,질문\na,같은 질문\nb,같은 질문\n", encoding="utf-8")
            rows = load_questions(path)
            self.assertEqual([row["id"] for row in rows], ["a", "b"])

    def test_multiturn_history(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "questions.json"
            write_json(path, [{"query": "자세히 알려줘", "conversation_history": [
                {"role": "user", "content": "NPS가 뭐야?"}, ["assistant", "추천 지표입니다."]]}])
            self.assertEqual(load_questions(path)[0]["conversation_history"][0], ["user", "NPS가 뭐야?"])

    def test_export_uses_final_answer_and_captured_evidence(self):
        state = {"final_answer": "최종 답변", "tools_used": ["read_manual"],
                 "evaluation_evidence": [{"tool": "read_manual", "context": "문서 원문"}],
                 "messages": [{"content": "내부 추론 및 시스템 프롬프트"}]}
        self.assertEqual(build_evaluation_case({"input": "질문"}, state), {
            "input": "질문", "response": "최종 답변", "tool": ["read_manual"], "context": ["문서 원문"]})

    def test_refusal_has_no_fabricated_context(self):
        self.assertEqual(build_evaluation_case({"input": "질문"}, {"final_answer": "조회 불가"})["context"], [])

    def test_multiturn_evaluation_input_contains_history(self):
        case = build_evaluation_case({"input": "자세히", "conversation_history": [["user", "NPS 질문"]]},
                                     {"final_answer": "답변"})
        self.assertIn("NPS 질문", case["input"])
        self.assertIn("[현재 질문]\n자세히", case["input"])

    def test_trace_includes_only_executed_calls(self):
        messages = [
            SimpleNamespace(tool_calls=[{"id": "one", "name": "query_nps", "args": {"year": 2026}},
                                        {"id": "two", "name": "not_executed", "args": {}}]),
            SimpleNamespace(type="tool", tool_call_id="one", name="query_nps", content="result"),
        ]
        trace = executed_tool_trace({"messages": messages})
        self.assertEqual([item["name"] for item in trace], ["query_nps"])
        self.assertEqual(trace[0]["args"], {"year": 2026})

    def test_missing_evidence_fails_instead_of_silently_dropping_grounding(self):
        with self.assertRaises(ValueError):
            build_evaluation_case({"input": "질문"}, {"final_answer": "답변", "tools_used": ["read_manual"]})

    def test_invalid_contract_is_rejected(self):
        for data in ([], [{"input": "질문", "actual_output": "잘못된 키"}],
                     [{"input": "질문", "response": "답변", "tool": ["x"], "context": []}]):
            with self.subTest(data=data), self.assertRaises(ValueError):
                validate_dataset(data)

    def test_original_create_test_cases_accepts_our_export(self):
        # Execute this unchanged function from source, with a tiny LLMTestCase stand-in.
        # The integration suite also checks it against the installed DeepEval class.
        source = Path(__file__).resolve().parents[3] / "deepeval" / "evaluate_agent_tool.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "create_test_cases")
        scope = {"List": list, "Dict": dict, "Any": object, "LLMTestCase": lambda **kwargs: SimpleNamespace(**kwargs)}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), scope)
        case = build_evaluation_case({"input": "질문"}, {"final_answer": "답변", "evaluation_evidence": [
            {"tool": "query_td_nps", "context": "NPS=42"}]})
        result = scope["create_test_cases"]([case])[0]
        self.assertEqual(result.actual_output, "답변")
        self.assertEqual(result.retrieval_context, ["NPS=42"])


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_failure_checkpoint_and_resume(self):
        class Agent:
            def __init__(self):
                self.calls = []
                self.fail = True

            async def execute_eval(self, query, **kwargs):
                self.calls.append(query)
                if query == "두 번째" and self.fail:
                    raise RuntimeError("network unavailable")
                return {"final_answer": f"답변: {query}"}

        questions = [{"id": "a", "input": "첫 번째", "conversation_history": []},
                     {"id": "b", "input": "두 번째", "conversation_history": []}]
        with TemporaryDirectory() as directory:
            output = Path(directory)
            agent = Agent()
            path, failures = await collect(agent, questions, output, "test-user", 1)
            self.assertEqual(failures, 1)
            self.assertEqual(len(json.loads(path.read_text())), 1)
            agent.fail = False
            path, failures = await collect(agent, questions, output, "test-user", 1, resume=True)
            self.assertEqual(failures, 0)
            self.assertEqual(agent.calls, ["첫 번째", "두 번째", "두 번째"])
            self.assertEqual(len(json.loads(path.read_text())), 2)
            summary = json.loads((output / "collection_summary.json").read_text())
            self.assertEqual(summary["case_mapping"][1]["question_id"], "b")

    async def test_timeout_is_excluded_from_dataset(self):
        class Agent:
            async def execute_eval(self, *args, **kwargs):
                await asyncio.sleep(1)

        with TemporaryDirectory() as directory:
            path, failures = await collect(Agent(), [{"id": "a", "input": "질문", "conversation_history": []}],
                                           Path(directory), "user", 0.001)
            self.assertEqual(failures, 1)
            self.assertEqual(json.loads(path.read_text()), [])

    async def test_history_and_user_context_reach_agent(self):
        received = []

        class Agent:
            async def execute_eval(self, query, **kwargs):
                received.append(kwargs)
                return {"final_answer": "답변"}

        context = []
        with TemporaryDirectory() as directory:
            await collect(Agent(), [{"id": "a", "input": "질문", "conversation_history": [["user", "앞선 질문"]]}],
                          Path(directory), "employee", 1, context_setter=context.append)
        self.assertEqual(received[0]["conversation_history"], [("user", "앞선 질문")])
        self.assertEqual(received[0]["user_id"], "employee")
        self.assertEqual(context, ["employee", None])

    async def test_truncated_last_journal_write_is_repaired(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "traces.jsonl"
            path.write_text('{"index":', encoding="utf-8")
            self.assertEqual(load_checkpoint(path, []), {})
            self.assertEqual(path.read_text(), "")


class AdapterTests(unittest.TestCase):
    def test_adapter_loads_source_readonly_and_writes_to_output_directory(self):
        # This tiny module verifies the adapter boundary without external packages.
        source_text = '''
import json
from pathlib import Path
AGENT_CONFIGS = {"cx": {"data_path": "unused.json"}}
def setup_logging(path):
    Path(path).touch()
def run_evaluation(agent_name):
    assert agent_name == "cx"
    assert Path(".deepeval").is_dir()
    data = json.loads(Path(AGENT_CONFIGS["cx"]["data_path"]).read_text())
    class Result:
        def model_dump(self, mode):
            return {"total_test_cases": len(data), "overall_pass_rate": 1,
                    "test_results": [{"metrics": [{"error": None}]}]}
    return Result()
'''
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "protected"
            source.mkdir()
            (source / "evaluate_agent_tool.py").write_text(source_text)
            data_path = root / "data.json"
            write_json(data_path, [{"input": "질문", "response": "답변"}])
            output = root / "output" / "results.json"
            before = fingerprint(source)
            self.assertEqual(evaluate_existing(data_path, source, output), 0)
            self.assertEqual(json.loads(output.read_text())["total_test_cases"], 1)
            self.assertTrue(output.with_suffix(".log").exists())
            self.assertEqual(fingerprint(source), before)
            with self.assertRaises(ValueError):
                evaluate_existing(data_path, source, source / "results.json")


if __name__ == "__main__":
    unittest.main()
