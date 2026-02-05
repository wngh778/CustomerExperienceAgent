import os
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parents[1]
QUERY_FOLDER_PATH  = os.path.join(BASE_DIR, "resources", "query")
PROMPT_FOLDER_PATH = os.path.join(BASE_DIR, "resources", "prompt")
TEMPLATE_FOLDER_PATH = os.path.join(BASE_DIR, "resources", "template")


def load_query(file_name: str) -> str:
    file_path = os.path.join(QUERY_FOLDER_PATH, file_name)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"[load_query] 파일을 찾을 수 없습니다: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    return content

def load_prompt(file_name: str) -> str:
    file_path = os.path.join(PROMPT_FOLDER_PATH, file_name)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"[load_prompt] 파일을 찾을 수 없습니다: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    return content

def load_template(file_name: str) -> str:
    file_path = os.path.join(TEMPLATE_FOLDER_PATH, file_name)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"[load_template] 파일을 찾을 수 없습니다: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    return content