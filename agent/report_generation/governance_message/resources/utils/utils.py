import sys, os, yaml, jinja2, asyncio
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple, Optional, Callable
from jinja2 import Environment, FileSystemLoader, select_autoescape
from core.logger import get_logger

logger = get_logger(__name__)

#################### 경로 ####################
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
RESOURCE_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
CONFIG_PATH = os.path.join(RESOURCE_DIR, "config", "governance_screens.yaml")
TEMPLATE_ROOT = os.path.join(CURRENT_DIR, "../", "templates")


#################### MCP ####################
_executor: Optional["MCPToolExecutor"] = None
_executor_lock = asyncio.Lock() 

MAX_CONCURRENT_QUERIES = int(os.getenv("MAX_CONCURRENT_QUERIES", "40"))
_query_semaphore = asyncio.Semaphore(MAX_CONCURRENT_QUERIES)


#################### 유틸 함수 ####################
_RENDERER_REGISTRY: Dict[str, Any] = {}

JINJA_ENV = Environment(
    loader=FileSystemLoader(searchpath=TEMPLATE_ROOT),
    autoescape=select_autoescape(["html", "xml"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


def register_renderer(template_name: str):
    """
    데코레이터 – 함수와 query_id 를 매핑
    """
    def decorator(func: Callable[[Any], str]) -> Callable[[Any], str]:
        _RENDERER_REGISTRY[template_name] = func
        return func
    return decorator


def render_template(template_name: str, data: Any) -> str:
    """
    템플릿 이름과 데이터를 받아 Jinja2 템플릿을 렌더링하여 반환.
    """
    if template_name not in _RENDERER_REGISTRY:
        raise FileNotFoundError(f"Renderer for '{template_name}' not found.")
    return _RENDERER_REGISTRY[template_name](data)


def _to_dict(data: Any) -> List[Dict[str, Any]]:
    """
    헬퍼: DataFrame / dict / list -> list[dict]로 변환
    """
    if isinstance(data, pd.DataFrame):
        return data.to_dict(orient="records")
    if isinstance(data, list):
        # 리스트 안에 리스트가 있으면 1단계 평탄화
        if any(isinstance(i, list) for i in data):
            flat = []
            for i in data:
                if isinstance(i, list):
                    flat.extend(i)
                else:
                    flat.append(i)
            data = flat
        if not all(isinstance(item, dict) for item in data):
            raise TypeError("list 안에 dict 가 아닌 요소가 있습니다.")
        return data
    if isinstance(data, dict):
        return [data]
    raise TypeError(f"지원되지 않는 데이터 타입: {type(data)}")


def extract_data_by_type(records: List[Dict[str, List[Any]]]) -> Dict[str, List[Any]]:
    """
    records 안에 들어있는 딕셔너리들의 키별 데이터를 하나의 리스트로 합쳐 반환한다.
    """
    data_by_type: Dict[str, List[Any]] = {}

    for record in records:
        for key, value in record.items():
            # value 가 리스트가 아니라면 리스트로 감싸서 처리
            if not isinstance(value, list):
                value = [value]

            # 해당 키가 아직 없으면 빈 리스트를 만든다
            if key not in data_by_type:
                data_by_type[key] = []

            # 기존 리스트에 현재 레코드의 값을 extend
            data_by_type[key].extend(value)

    return data_by_type


def safe_float(val: Any, default: float = 0.0) -> float:
    """
    None, NaN, 혹은 문자열을 안전하게 float 으로 변환
    """
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def get_query_file(query_name: str) -> str:
    """
    resource/query 폴더 아래에 있는 *.sql 파일 절대 경로 반환
    """
    return os.path.join(RESOURCE_DIR, "query", f"{query_name}.sql")


def read_sql_file(filename: str) -> str:
    with open(filename, "r", encoding="utf-8") as f:
        return f.read()


async def run_query_async(
    executor,
    query_id: str,
    params: Dict[str, Any],
) -> Tuple[str, Any]:
    """
    `query_id` 와 이미 병합된 `params` 를 받아서 쿼리 실행
    """
    query_file = get_query_file(query_id)

    if not os.path.exists(query_file):
        print(f"[WARN] 쿼리 파일이 없습니다: {query_file}")
        return query_id, None

    try:
        raw_sql = read_sql_file(query_file)
        rendered_sql = jinja2.Template(raw_sql).render(**params)
    except Exception as e:
        logger.error(f"[run_query_async] render 중 오류가 발생했습니다. 파일명: {query_file}, 파라미터: {params}")
        return query_id, e              # 예외 객체를 그대로 반환 → 나중에 처리

    async with _query_semaphore:
        try:
            result = await executor.execute_tool("mysql_query", {"query": rendered_sql})
            return query_id, result
        except Exception as e:
            logger.error(f"[run_query_async] mcp 실행 중 오류가 발생했습니다.\n쿼리:\n{rendered_sql}")
            return query_id, e


def load_screen_config() -> Dict[str, Any]:
    """
    governance_screens.yaml 로드
    """

    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        return cfg
    except yaml.YAMLError as exc:
        raise exc


def find_screen_by_path(
    cfg: Dict[str, Any], path: str
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]]:
    """
    path 가 빈 문자열이거나 "." 만 들어오면
    - parent_node   : None
    - target_node   : cfg["screens"] (전체 화면 트리)
    - node_path     : []   (루트 레벨이므로 파라미터 병합 대상이 없음)
    """
    if not path or path == ".":
        return None, cfg.get("screens", {}), []

    parts = path.split(".")
    node = cfg.get("screens", {})
    parent = None
    node_path: List[Dict[str, Any]] = []

    parts = path.split(".")
    node = cfg.get("screens", {})
    parent = None
    node_path: List[Dict[str, Any]] = []

    for part in parts:
        if isinstance(node, dict) and part in node:
            parent = node
            node = node[part]
            node_path.append(node)
        else:
            return None
    return parent, node, node_path


def merge_params(node_path: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    node_path 에 있는 모든 `_params` 를 차례대로 병합
    """
    merged: Dict[str, Any] = {}
    for node in node_path:
        params = node.get("_params", {})
        if not isinstance(params, dict):
            raise ValueError("'_params' must be a dict.")
        merged.update(params)
    return merged


def merge_codes(node_path: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    node_path 에 있는 모든 `_code` 를 차례대로 병합
    """
    merged: Dict[str, str] = {}
    for node in node_path:
        code_block = node.get("_code", {})
        if not isinstance(code_block, dict):
            raise ValueError("'_code' must be a dict.")
        merged.update({k: str(v) for k, v in code_block.items()})
    return merged

def extract_query_id(screen_node: Dict[str, Any]) -> List[str]:
    """
    leaf 노드에 정의된 query_id 를 리스트 형태로 반환
    - 단일 문자열이면 [string] 로 감싸서 반환
    - 리스트이면 그대로 반환
    """
    if "query_id" not in screen_node:
        raise ValueError("해당 화면에 'query_id' 가 정의되지 않았습니다.")
    q = screen_node["query_id"]
    if isinstance(q, list):
        if not q:
            raise ValueError("query_id 리스트가 비어 있습니다.")
        return [str(item) for item in q]
    return [str(q)]


def _round_diff(base: float, target: float) -> float:
    """
    두 값의 절대 차이를 소수점 한 자리로 반올림
    """
    return round((base - target), 1)


def _flag_high_low(base: float, target: float, suffix: bool) -> str:
    """
    suffix bool 여부에 따라 접미사 결정
    base > target 이면 `높고` 또는 `높음` 반환
    base < target 이면 `낮고` 또는 `낮음` 반환
    """
    diff = _round_diff(base, target)
    if suffix:
        if diff > 0.0:
            return "높고"
        elif diff == 0.0:
            return "같고"
        else:   # diff < 0.0
            return "낮고"
    else:
        if diff > 0.0:
            return "높음"
        elif diff == 0.0:
            return "같음"
        else:   # diff < 0.0
            return "낮음"


def diff_and_flag(
    base: float,                    # 기준 값
    target: float,                 # 비교할 값
    suffix: bool,                   # 접미사 여부
) -> str:
    """
    편차 및 플래그 계산
    """
    diff = _round_diff(base, target)
    flag = _flag_high_low(base, target, suffix)

    return diff, flag


def has_batchim(char: str) -> bool:
    if not char:
        return False
    code = ord(char)
    if 44032 <= code <= 55203:
        return (code - 44032) % 28 != 0
    return False