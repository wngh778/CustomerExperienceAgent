"""Offline integration: real LangGraph/DeepEval, deterministic LLM and tool responses."""

from __future__ import annotations

import importlib.metadata
import importlib.util
import json
import logging
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from evaluation.dataset import build_evaluation_case, executed_tool_trace, write_json
from evaluation.evaluate import fingerprint

MAIN_DIR = Path(__file__).resolve().parents[2]
DEEPEVAL_DIR = MAIN_DIR.parent / "deepeval"


def installed(package):
    try:
        importlib.metadata.version(package)
        return True
    except importlib.metadata.PackageNotFoundError:
        return False


@unittest.skipUnless(installed("langgraph") and installed("pandas"), "LangGraph/pandas 통합 환경에서 실행")
class GraphIntegrationTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        sys.path.insert(0, str(MAIN_DIR / "app"))
        config = ModuleType("core.config")
        config.settings = SimpleNamespace(UNSUPPORTED_DATA_MSGS={"VOC_TYPE_SENTIMENT": "정책으로 조회 불가"})
        util = ModuleType("core.util")
        util.create_azurechatopenai = MagicMock()
        # Mock environment/LLM construction; graph, nodes, messages and evidence formatter are real.
        configuration_patch = patch.dict(sys.modules, {"core.config": config, "core.util": util})
        configuration_patch.start()
        cls.addClassCleanup(configuration_patch.stop)
        from agent.report_generation.report_generation_agent import ReportGenerationAgent
        cls.agent_class = ReportGenerationAgent

    def make_agent(self, responses, tools=()):
        agent = object.__new__(self.agent_class)
        agent.logger = logging.getLogger("evaluation.integration")
        agent.llm = MagicMock()
        agent._cx_hierarchy = "테스트 고객경험 체계"
        agent._nps_tools = list(tools)
        agent._call_llm = AsyncMock(side_effect=responses)
        agent._load_department_list = AsyncMock(return_value="테스트 부서")
        agent._load_latest_data_info = AsyncMock(return_value="2026-01")
        agent.graph = agent._build_graph()
        return agent

    async def test_sql_graph_records_only_executed_data_and_same_50_rows(self):
        from langchain_core.messages import AIMessage
        from langchain_core.tools import tool
        from agent.report_generation.resources.models import IntentClassification, PolicyGuardResult

        executions = []
        data = [{"순번": i, "고객경험요소": f"ROW_{i}_END", "NPS": 42.1234} for i in range(100)]

        @tool
        def query_nps() -> str:
            """Return deterministic NPS data."""
            executions.append("query_nps")
            return json.dumps({"view_id": "td_nps", "sql": "SELECT test", "data": data,
                               "row_count": len(data), "error": None})

        agent = self.make_agent([
            IntentClassification(intent="nps_analysis", reason="분석"),
            PolicyGuardResult(is_violation=False, reason="채널 조회"),
            AIMessage(content="", tool_calls=[{"name": "query_nps", "args": {}, "id": "executed"}]),
            AIMessage(content="", tool_calls=[{"name": "not_executed", "args": {}, "id": "planned"}]),
            AIMessage(content="NPS는 42.12입니다."),
        ], [query_nps])
        state = await agent.execute_eval("NPS를 알려줘", user_id="test")
        case = build_evaluation_case({"input": "NPS를 알려줘"}, state)
        self.assertEqual(executions, ["query_nps"])
        self.assertEqual(case["tool"], ["query_nps"])
        self.assertIn("ROW_49_END", case["context"][0])
        self.assertNotIn("ROW_50_END", case["context"][0])
        self.assertIn("42.12", case["context"][0])
        self.assertEqual([entry["name"] for entry in executed_tool_trace(state)], ["query_nps"])

    async def test_manual_graph_keeps_actual_document_and_hierarchy(self):
        from langchain_core.messages import AIMessage
        from agent.report_generation.resources.models import IntentClassification

        agent = self.make_agent([IntentClassification(intent="manual", reason="정의"),
                                 AIMessage(content="NPS 설명입니다.")])
        with patch("agent.report_generation.report_generation_agent.read_manual",
                   return_value={"content": "테스트 매뉴얼 원문", "success": True}):
            state = await agent.execute_eval("NPS가 뭐야?", conversation_history=[("user", "이전 대화")])
        case = build_evaluation_case({"input": "NPS가 뭐야?"}, state)
        self.assertEqual(case["tool"], ["read_manual"])
        self.assertIn("테스트 매뉴얼 원문", case["context"][0])
        self.assertIn("테스트 고객경험 체계", case["context"][0])
        self.assertEqual(state["conversation_history"], [("user", "이전 대화")])

    async def test_unsafe_graph_has_no_context(self):
        from agent.report_generation.resources.models import IntentClassification

        agent = self.make_agent([IntentClassification(intent="unsafe", reason="개인정보")])
        state = await agent.execute_eval("고객 전화번호 알려줘")
        case = build_evaluation_case({"input": "고객 전화번호 알려줘"}, state)
        self.assertEqual(case["context"], [])
        self.assertEqual(agent._call_llm.await_count, 1)

    async def test_blocked_query_does_not_ground_a_policy_refusal_in_empty_rows(self):
        agent = self.make_agent([])
        state = {"current_query": "감정 분석", "tools_used": ["blocked_voc_tool"],
                 "query_results": [{"purpose": "UNSUPPORTED_DATA-VOC_TYPE_SENTIMENT", "data": []}]}
        result = await agent._nps_analyst(state)
        case = build_evaluation_case({"input": "감정 분석"}, {**state, **result})
        self.assertEqual(case["response"], "정책으로 조회 불가")
        self.assertEqual(case["context"], [])
        agent._call_llm.assert_not_awaited()


@unittest.skipUnless(installed("deepeval") and installed("langchain-openai"), "DeepEval 통합 환경에서 실행")
class EvaluatorIntegrationTests(unittest.TestCase):
    def test_unchanged_evaluator_runs_real_metrics_with_deterministic_judge(self):
        os.environ["DEEPEVAL_TELEMETRY_OPT_OUT"] = "1"
        from deepeval.models import DeepEvalBaseLLM

        class Judge(DeepEvalBaseLLM):
            def load_model(self):
                return self

            def get_model_name(self):
                return "deterministic-test-judge"

            def generate(self, prompt, schema=None):
                fields = schema.model_fields
                values = {}
                for field in fields:
                    values[field] = {
                        "statements": ["NPS is 42."], "truths": ["NPS is 42."],
                        "claims": ["NPS is 42."], "opinions": [],
                        "verdicts": [{"verdict": "yes", "reason": "Fixed test verdict"}],
                        "steps": ["Check grammar", "Check clarity"],
                        "score": 10, "reason": "Fixed test reason",
                    }[field]
                return schema(**values)

            async def a_generate(self, prompt, schema=None):
                return self.generate(prompt, schema)

        before = fingerprint(DEEPEVAL_DIR)
        old_cwd, old_path, old_bytecode = Path.cwd(), sys.path[:], sys.dont_write_bytecode
        try:
            sys.dont_write_bytecode = True
            sys.path.insert(0, str(DEEPEVAL_DIR))
            with TemporaryDirectory() as directory:
                output = Path(directory)
                (output / ".deepeval").mkdir()
                os.chdir(output)
                spec = importlib.util.spec_from_file_location("integration_original_evaluator", DEEPEVAL_DIR / "evaluate_agent_tool.py")
                evaluator = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = evaluator
                spec.loader.exec_module(evaluator)
                data = [
                    {"input": "What is NPS?", "response": "NPS is 42.", "tool": [], "context": []},
                    {"input": "What is NPS?", "response": "NPS is 42.",
                     "tool": ["query_nps"], "context": ["NPS is 42."]},
                ]
                write_json(output / "input.json", data)
                evaluator.AGENT_CONFIGS["cx"]["data_path"] = str(output / "input.json")
                evaluator.logger = evaluator.setup_logging(str(output / "evaluation.log"))
                with patch.object(evaluator, "AzureOpenAIEvaluator", return_value=Judge()):
                    result = evaluator.run_evaluation("cx")
                self.assertEqual(result.total_test_cases, 2)
                self.assertEqual(len(result.test_results[0].metrics), 3)
                self.assertEqual(len(result.test_results[1].metrics), 4)
                self.assertFalse(any(metric.error for case in result.test_results for metric in case.metrics))
        finally:
            os.chdir(old_cwd)
            sys.path[:] = old_path
            sys.dont_write_bytecode = old_bytecode
        self.assertEqual(fingerprint(DEEPEVAL_DIR), before)


if __name__ == "__main__":
    unittest.main()
