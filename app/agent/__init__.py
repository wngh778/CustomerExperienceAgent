from typing import Dict, Callable

from core.mcp_util import MCPToolExecutor
from core.config import settings
from .voc_management import VocManagementAgent
from .data_analysis import DataAnalysisAgent
from .report_generation import DocumentReportAgent, GovernanceMessage

AGENT_REGISTRY: Dict[str, Callable] = {
    "voc": VocManagementAgent(),
    "analysis": DataAnalysisAgent(),
    "doc_report": DocumentReportAgent(),
    "governance": GovernanceMessage(),
}

def get_agent(agent_type:str, mcp_executor:MCPToolExecutor):
    agent = AGENT_REGISTRY.get(agent_type)
    if agent is not None:
        agent.mcp_executor = mcp_executor
        return agent