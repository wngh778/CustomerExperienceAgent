from abc import ABC, abstractmethod
from datetime import datetime

from core.logger import get_logger
from core.util import create_azurechatopenai
from core.config import settings

import pytz

logger = get_logger(__name__)

class Agent(ABC):
    def __init__(self, prompt_path:str="./resources", tool_description_path:str="./resources"):
        self.prompt_path = prompt_path
        self.tool_description_path = tool_description_path

        self.today_date = datetime.now(pytz.timezone("Asia/Seoul")).date()
        
        self.llm = create_azurechatopenai(model_name=settings.MODEL_NAME)
        self.logger = get_logger(__name__)
        
        self.AZURE_OPENAI_API_KEY = settings.AZURE_OPENAI_API_KEY
        self.initialize_agent()

    def initialize_agent(self):
        """agent 초기화"""
        self.load_resources()
        self.define_tools()
        self.define_chains()

    @abstractmethod
    def load_resources(self):
        """tool description 또는 prompt 등의 resource file load"""

    def define_tools(self):
        """tool 정의"""
        pass

    def define_chains(self):
        """chain 정의 (유사 mini agent)"""
        pass

    @abstractmethod
    def execute(self, user_id:str, messages:list, today_date):
        """에이전트 실행 추상 메서드 (디테일 구현 나중에)"""

    