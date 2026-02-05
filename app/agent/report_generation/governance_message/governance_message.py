import re, os, sys, asyncio, json, pytz
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional, AsyncGenerator
from .resources.utils.renderers import _RENDERER_REGISTRY
from .resources.utils.utils import (
    render_template,
    load_screen_config,
    find_screen_by_path,
    merge_params,
    merge_codes,
    extract_query_id,
    run_query_async,
)
from core.logger import get_logger

logger = get_logger(__name__)

# 프로젝트 루트 경로
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASE_DIR)

class GovernanceMessage:
    """
    거버넌스 메시지를 API 규격에 맞는 JSON 형태로 반환하는 클래스
    """
    def __init__(self, executor: Optional[Any] = None) -> None: 
        self.mcp_executor = executor 
        
    KST = pytz.timezone("Asia/Seoul")

    # 1) leaf(쿼리) 경로 모두 수집
    @staticmethod
    def _collect_all_query_paths(
        node: Dict[str, Any], cur_path: List[str]
    ) -> List[str]:
        """
        현재 노드가 leaf(쿼리) 인 경우 cur_path 를 문자열로 반환하고,
        아니라면 자식들을 재귀 탐색해 모든 leaf 경로를 모음.
        """
        paths: List[str] = []

        if "query_id" in node:
            paths.append(".".join(cur_path))

        for key, child in node.items():
            if isinstance(child, dict) and key != "_params":
                paths.extend(
                    GovernanceMessage._collect_all_query_paths(
                        child, cur_path + [key]
                    )
                )
        return paths

    # 2) 화면 경로 → (query_id, merged_params) 로 변환 + 실행
    async def _iterate_query_results(
        self,
        screen_path: str,
        override: Optional[Dict[str, Any]] = None,
    ) -> AsyncGenerator[Tuple[str, str, Dict[str, str]], None]:
        """
        화면 경로(또는 여러 경로) 를 받아 leaf‑query 를 실행하고,
        템플릿을 렌더링한 문자열을 **yield** 합니다.
        반환값: (leaf_path, rendered_msg, code_dict)
        """
        cfg = load_screen_config()

        # 입력을 리스트 형태로 정규화
        if isinstance(screen_path, str):
            screen_path = [
                p.strip() for p in screen_path.split(",") if p.strip()
            ]

        # 빈 리스트이면 전체 화면 트리를 의미하도록 루트 경로 추가
        if not screen_path:
            screen_path = [""]   # "" 은 전체 화면 트리

        # leaf‑query 를 수집하고 실행
        leaf_paths: List[str] = []
        for sp in screen_path:
            result = find_screen_by_path(cfg, sp)
            if not result:
                continue

            _, target_node, node_path = result
            leaf_paths.extend(
                self._collect_all_query_paths(
                    target_node, sp.split(".") if sp else []
                )
            )

        # leaf 별 실행 정보 준비
        leaf_jobs: List[
            Tuple[str, List[str], Dict[str, Any], Dict[str, str]]
        ] = []

        # 화면 전체에 적용되는 파라미터
        base_params = merge_params(node_path)

        # 각 leaf 에 대해 파라미터·코드·query_id 를 정리
        for leaf_path in leaf_paths:
            leaf_res = find_screen_by_path(cfg, leaf_path)
            if not leaf_res:
                continue
            _, leaf_node, leaf_node_path = leaf_res

            leaf_params = merge_params(leaf_node_path)
            leaf_params.update(base_params)          # 화면‑레벨 파라미터와 병합
            if override:
                leaf_params.update(override)

            query_ids = extract_query_id(leaf_node)   # List[str]
            code_dict = merge_codes(leaf_node_path)   # depth‑code 매핑

            leaf_jobs.append(
                (leaf_path, query_ids, leaf_params, code_dict)
            )

        # 실제 쿼리 실행 (비동기)
        async def _run_one(
            leaf_path: str, qid: str, params: Dict[str, Any]
        ) -> Tuple[str, str, Any]:
            """leaf_path, query_id, result 를 반환"""
            _, result = await run_query_async(self.mcp_executor, qid, params)
            return leaf_path, qid, result

        all_tasks = [
            _run_one(lp, qid, lp_params)
            for (lp, qids, lp_params, _) in leaf_jobs
            for qid in qids
        ]

        raw_results: List[Tuple[str, str, Any]] = await asyncio.gather(
            *all_tasks, return_exceptions=True
        )

        # leaf_path 단위로 결과 재조합
        leaf_result_map: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for leaf_path, qid, result in raw_results:
            if isinstance(result, Exception):
                continue
            leaf_result_map[leaf_path][qid] = result

        # 템플릿 렌더링 + 최종 yield
        for leaf_path, _, _, code_dict in leaf_jobs:
            query_data = leaf_result_map.get(leaf_path, {})
            try:
                tmpl_name = find_screen_by_path(cfg, leaf_path)[1].get(
                    "template"
                )
                if not tmpl_name:
                    raise ValueError(
                        f"'{leaf_path}'에 템플릿 정보가 없습니다."
                    )
                rendered = render_template(tmpl_name, query_data)
            except Exception as e:
                logger.error(f"[_iterate_query_results]거버넌스 메세지 렌더링 중 오류 발생({code_dict}): {e}")
                rendered = ""

            yield leaf_path, rendered, code_dict, query_data

    # 3) 템플릿 호출 (단일 leaf_path 용)
    async def generate_output(
        self, leaf_path: str, data: Dict[str, Any]
    ) -> str:
        """
        leaf_path 에 해당하는 템플릿을 렌더링하고 문자열을 반환.
        """
        cfg = load_screen_config()
        result = find_screen_by_path(cfg, leaf_path)
        if not result:
            raise ValueError(
                f"'{leaf_path}'에 해당하는 화면 구성을 찾을 수 없습니다."
            )
        _, target_node, _ = result

        template_name = target_node.get("template")
        if not template_name:
            raise ValueError(f"'{leaf_path}'에 템플릿 정보가 없습니다.")

        return render_template(template_name, data)

    # 4) content 객체 생성 (page 안에 들어갈 단일 아이템)
    @staticmethod
    def _build_content_item(
        leaf_path: str,
        render_output: str,
        render_detail_output: str,
        code_dict: Dict[str, str],
        base_ym: str,
        base_dt: str,
    ) -> Dict[str, Any]:
        """
        API 규격의 `content` 부분을 만든다.
        - `code_dict` 에는 depth‑code 가 들어 있다.
        - `base_ym` 은 YYYYMM 형태 (쿼리 결과에서 추출하거나 요청일 기준)
        """
        return {
            "baseYm": base_ym,
            "baseDt": base_dt,
            "polModDstcd": code_dict.get("설문조사방식구분", ""),
            "polKndDstcd": code_dict.get("설문조사종류구분", ""),
            "polTagetDstcd": code_dict.get("설문조사대상구분", ""),
            "gvrnDstcd": code_dict.get("거버넌스구분", ""),
            "gvrnMsgCtnt": render_output,
            "gvrnMsgDetailCtnt": render_detail_output,
        }

    # 5) 페이지/메타데이터 생성
    @staticmethod
    def _build_page(
        contents: List[Dict[str, Any]],
        page_number: int = 0,
        page_size: int = 500,
    ) -> Dict[str, Any]:
        """
        `contents` 리스트를 받아 페이지 정보를 만든다.
        """
        total = len(contents)
        total_pages = (total - 1) // page_size + 1 if total else 1

        start = page_number * page_size
        end = start + page_size
        page_content = contents[start:end]

        return {
            "pageNumber": page_number,
            "pageSize": page_size,
            "totalElements": total,
            "totalPages": total_pages,
            "lastYn": page_number == total_pages - 1,
            "firstYn": page_number == 0,
            "numberOfElements": len(page_content),
            "content": page_content,
        }

    # 6) 거버넌스 메시지 regex에 따라 gvrnMsgCtnt / gvrnMsgDetailCtnt 분류
    @staticmethod
    def _split_rendered_msg(rendered: str) -> Tuple[str, str]:
        # rendered_msg 에서 regex: {detailContents}
        # {detailContents} 전의 rendered_msg = seperate_msg
        # {detailContents} 후의 rendered_msg = seperate_detail_msg

        parts = re.split(r"\{detailContents\}", rendered, maxsplit=1)

        if len(parts) == 2:
            # 앞부분과 뒷부분을 공백 없이 반환
            return parts[0].strip(), parts[1].strip()
        else:
            # 패턴이 없으면 전체를 앞부분에 넣고 뒤부분은 빈 문자열
            return rendered.strip(), ""

    # 7) 최종 API 응답 구조
    @staticmethod
    def _build_api_response(
        page: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        최종 응답 JSON을 만든다.
        """
        metadata = {
            "prcssMdelVsnId": "20",
            "prcssYmYMS": datetime.now(GovernanceMessage.KST).strftime("%Y%m%d%H%M%S"),
        }

        return {"metadata": metadata, "page": page}

    # -----------------------------------------------------------------
    # 8) 날짜 → 학기 변환 헬퍼
    # -----------------------------------------------------------------
    @staticmethod
    def _parse_date_to_semester(date_str: str) -> Tuple[str, str, str]:
        if len(date_str) != 8 or not date_str.isdigit():
            raise ValueError("date must be in YYYYMMDD format")
        year = date_str[:4]
        month = date_str[4:6]
        semester = "상반기" if int(month) < 7 else "하반기"
        return year, month, semester

    # 8) 메인 엔트리 – API 요청을 받아 전체 거버넌스 메시지를 만든다
    async def generate_governance(
        self, request_body: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        - `request_body` : 클라이언트가 POST 로 보낸 JSON
        - 반환값 : API 규격에 맞는 JSON (page 하나 혹은 여러 페이지)
        """
        try:
            # ① contents 검증 (size 1)
            contents = request_body.get("contents", [])
            if not isinstance(contents, list) or len(contents) != 1:
                raise ValueError("contents must be a list of size 1")
    
            content = contents[0]
            request_type = content.get("requestType")
            if request_type == "":
                raise ValueError("unsupported requestType")

            from importlib import resources
            raw_sql = resources.read_text(
                "agent.report_generation.governance_message.resources.query",
                "td_date.sql"
            )
            recent_data_date = await self.mcp_executor.execute_tool(
                "mysql_query", {"query": raw_sql }
            )

            year, month, semester = self._parse_date_to_semester(
                content.get("date", "")
            )

            if recent_data_date:
                first_item = recent_data_date[0]
                override = {
                    "survey_year": int(first_item["조사년도"]),
                    "survey_month": datetime.now(GovernanceMessage.KST).strftime("%m"),
                    "semester": first_item["반기구분"],
                }
            else:
                override = {
                    "survey_year": year.strip("'"),
                    "survey_month": month.strip("'"),
                    "semester": semester,
                }

            # 화면 경로 (전체 조회) → 최상위 key 사용 content.get("requestType") 해당 값에 따라 실행되는 depth가 달라져야함
            # requestType = governance  -> 전체
            # requestType = top_down -> TD 만 실행
            # requestType = bottom_up -> BU 만 실행
            screen_path = ""
            if request_type == "governance":
                screen_path = ""
            else:
                screen_path = request_type
    
            # leaf 순회 → content 객체 수집
            all_contents: List[Dict[str, Any]] = []
            async for leaf_path, rendered_msg, code_dict, query_data in self._iterate_query_results(
                screen_path, override
            ):
                base_ym = f"{year}{month}"
                base_dt = datetime.now(GovernanceMessage.KST).strftime("%Y%m%d%H%M%S")

                separate_msg, separate_detail_msg = GovernanceMessage._split_rendered_msg(rendered_msg)

                item = self._build_content_item(
                    leaf_path=leaf_path,
                    render_output=separate_msg,
                    render_detail_output=separate_detail_msg,
                    code_dict=code_dict,
                    base_ym=base_ym,
                    base_dt=base_dt,
                )
                all_contents.append(item)

            # 페이지 구성
            page = self._build_page(
                contents=all_contents,
                page_number=content.get("page", 0),   # 현재는 0 고정
                page_size=100,
            )

            # 최종 응답 반환
            res = self._build_api_response(page=page)
            return {
                "success": True,
                "response": res
            }
        except Exception as exc:
            return {
                "success": False,
                "error": str(exc),
            }


# 파일이 직접 실행될 때 (테스트용)
if __name__ == "__main__":
    import json

    sample_req = {
        "isStream": False,
        "agentId": "652",
        "contents": [
            {
                "requestType": "governance",
                "page": 0,
                "date": "20251225",
            }
        ],
    }

    async def _run():
        gm = GovernanceMessage()
        resp = await gm.generate_governance(sample_req)
        print(json.dumps(resp, ensure_ascii=False, indent=2))

    asyncio.run(_run())