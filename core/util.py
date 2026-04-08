import re
import os
import json
import requests
import random
import string
import httpx

from pathlib import Path
from langgraph.graph import END
from watchdog.observers import Observer
from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI, ChatOpenAI
from core.config import settings
from core.pii_masking import check_pii
from core.custom_aoai import AzureChatOpenAIWithDynamicHeaders

from .logger import get_logger

logger = get_logger(__name__)

def create_simple_agent(llm, system_prompt="", output_structure=None):
        prompt_template = ChatPromptTemplate.from_messages(
                [
                    ("system", system_prompt),
                ]
            )

        if output_structure is None:
            agent = prompt_template | llm | StrOutputParser()
        else:
            structured_output = llm.with_structured_output(output_structure)
            agent = prompt_template | structured_output
        return agent

def load_resource_file(file_path, read_type="r"):
    if file_path is not None and os.path.exists(file_path):            
        with open(file_path, read_type) as f:
            if ".json" in file_path:
                return json.load(f)
            else:
                return f.read()
    else:
        assert False, f"파일 경로가 올바르지 않습니다. env 파일을 확인하세요.\n현재 경로: {file_path}"

def append_to_system_prompt(messages, extra_instruction):
    if messages and messages[0]["role"] == "system":
        if extra_instruction not in messages[0]["content"]:
            messages[0]["content"] += f"\n\n**{extra_instruction}**"
    else:
        messages.insert(0, {"role": "system", "content": extra_instruction})
    return messages

def add_random_char(user_id):
    random_str = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(5))
    return f"{user_id}-{random_str}"

def create_azurechatopenai(headers:dict={"kb-key": settings.AZURE_OPENAI_API_KEY}, user_id:str=settings.MCP_USER_ID, model_name:str="gpt-5", reasoning_effort:str="minimal"):
    def random_user_header():
        nonlocal user_id
        if not hasattr(random_user_header, "is_first"):
            x_client_user = user_id
        else:
            x_client_user = add_random_char(user_id)
        if not hasattr(random_user_header, "is_first"):
            random_user_header.is_first = False
        return {"x-client-user": x_client_user}

    if model_name == "gpt-5":
        endpoint = settings.AZURE_OPENAI_ENDPOINT + "/gpt-5"
    elif model_name == "gpt-5-mini":
        endpoint = settings.AZURE_OPENAI_ENDPOINT + "/gpt-5-mini"

    llm = AzureChatOpenAIWithDynamicHeaders(
        model_name=model_name,
        openai_api_version=settings.OPENAI_API_VERSION,
        api_version=settings.API_VERSION,
        default_headers=headers,
        max_retries=settings.MAX_RETRIES,
        timeout=settings.TIMEOUT,
        azure_endpoint=endpoint,
        reasoning_effort=reasoning_effort,
        api_key=settings.AZURE_OPENAI_API_KEY,
        header_generation_callback=random_user_header
    )
    return llm

def convert_input_messages(messages:list[dict]):
    result = []
    for m in messages:
        if m["role"] != "user":
            result.append(("assistant", m["content"]))
        else:
            result.append(("user", m["content"]))
    return result

def keep_latest_files(folder_path, keep=90):
    folder = Path(folder_path)
    files = [f for f in folder.iterdir() if f.is_file()]
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

    for file in files[keep:]:
        try:
            file.unlink()
            logger.info(f"{file.name} 삭제됨")
        except Exception as e:
            logger.error(f"{file.name} 삭제 실패, {e}")

def check_pii_in_chat_history(messages:list[dict]):
    for m in messages:
        if check_pii(m.get("content")):
            return True
    return False

def pydantic_to_description_json(model) -> dict:
    schema = model.model_json_schema()
    props = schema["properties"]
    
    return {
        field: info.get("description", "")
        for field, info in props.items()
    }