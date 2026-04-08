from typing import Callable, Dict, Union

from agent.agent_template import Agent
from core.util import load_resource_file
from core.config import settings
from core.logger import get_logger
# ── 실제 보고서 생성 함수들 import (Static) ───────────────────────
from .reports.report_types.td_nps_report import generate_td_nps_report
from .reports.report_types.bu_nps_weekly_report import generate_bu_nps_weekly_report
from .reports.report_types.bu_nps_monthly_report import generate_bu_nps_monthly_report
from .reports.report_types.region_group_nps_biweekly_report import generate_region_group_nps_biweekly_report

import asyncio
import os

logger = get_logger("doc_report_agent")

default_resource_path = "/".join(os.path.abspath(__file__).split("/")[:-1])

class DocumentReportAgent(Agent):
    """
    보고서 타입 → 생성 함수 매핑을 관리하고,
    파일 경로(str) 를 반환한다.
    """
    def __init__(self, prompt_path:str=default_resource_path+"/reports/resources/prompt", tool_description_path:str=default_resource_path+"/tool_description") -> None:
        self.prompts = {}
        
        # type: Callable[..., Union[str, asyncio.Future]]
        self._generators: Dict[str, Callable[..., Union[str, asyncio.Future]]] = {
            "td_nps_report": generate_td_nps_report,
            "bu_nps_weekly_report": generate_bu_nps_weekly_report,
            "bu_nps_monthly_report": generate_bu_nps_monthly_report,
            "region_group_nps_biweekly_report": generate_region_group_nps_biweekly_report,
        }
        super().__init__(prompt_path, tool_description_path)
        self.mcp_executor = None

    def load_langfuse_resources(self):
        return

    def load_resources(self):
        for p_name in os.listdir(self.prompt_path):
            if p_name[-4:] == ".txt":
                self.prompts[p_name] = load_resource_file(self.prompt_path + "/" + p_name)
    # -----------------------------------------------------------------
    async def generate(self, user_id: str, report_type: str, today_date) -> str:
        """
        지정된 report_type 의 보고서를 생성하고 저장된 파일 경로를 반환한다.
        """
        if report_type not in self._generators:
            raise ValueError(
                f"지원되지 않는 보고서 타입: {report_type}. "
                f"가능한 타입: {list(self._generators)}"
            )

        gen_func = self._generators[report_type]
        try:
            file_path = await gen_func(self.mcp_executor, user_id, self.llm, self.prompts, self.today_date)
        except Exception as exc:
            logger.exception(f"보고서 생성 중 에러 발생")
            raise exc

        return str(file_path)

    # -----------------------------------------------------------------
    def get_available_types(self) -> list[str]:
        """현재 등록된 보고서 타입 리스트 반환"""
        return list(self._generators.keys())

    async def execute(self, user_id:str, report_type:str, today_date):
        if self.today_date != today_date:
            self.today_date = today_date

        available_types = self.get_available_types()
        if report_type not in available_types:
            err_msg = f"가능한 보고서 타입이 아닙니다. 가능한 타입: {available_types}"
            logger.error(err_msg)
            raise ValueError(err_msg)

        try:
            result_path = await self.generate(user_id, report_type, self.today_date)
            return {
                "success": True,
                "user_id": user_id,
                "report_type": report_type,
                "result": result_path
            }
        except ValueError as e:
            return {
                "success": False,
                "error": str(e)
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"보고서 생성 실패: {str(e)}"
            }
