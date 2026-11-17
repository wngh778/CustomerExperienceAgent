from typing import Dict, Callable

from core.mcp_util import MCPToolExecutor
from core.config import settings
from .report_generation import ReportGenerationAgent
from .sql_agent import SQLAgent

AGENT_REGISTRY: Dict[str, Callable] = {
    "report": ReportGenerationAgent(),
    "sql": SQLAgent()
}

def get_agent(agent_type:str, mcp_executor:MCPToolExecutor):
    if agent_type == "report":
        agent = ReportGenerationAgent(mcp_executor=mcp_executor)
    else:
        agent = AGENT_REGISTRY.get(agent_type)
    if agent is not None:
        agent.mcp_executor = mcp_executor
        return agent