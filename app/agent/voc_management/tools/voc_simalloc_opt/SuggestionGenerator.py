from agent.voc_management.utils.load_files import load_prompt
from langchain_core.messages import SystemMessage
from typing import Dict, Any, List, Tuple
from datetime import datetime
import pytz


class SuggestionGenerator:
    """
    VOC 데이터의 기존 검토 정보와 LLM 결과를 종합하여
    고객별 개선 의견, 검토구분 코드, 요약 등을 생성하고 리포트를 출력하는 도구입니다.
    """

    code_to_kor = {"01": "현행유지", "02": "개선예정", "03": "개선불가"}

    @staticmethod
    def parse_llm_response(text: str) -> Tuple[str, str]:
        """
        LLM 응답 텍스트에서 '개선의견'과 '과제검토구분코드/과제검토구분'을 파싱합니다.
        - 개선의견: '개선의견:' 접두어 라인
        - 코드: '과제검토구분코드:' 혹은 '과제검토구분:'(한글값을 코드로 매핑)
        """
        if not text:
            return "", ""
        improvement, code = "", ""
        kor_to_code = {v: k for k, v in SuggestionGenerator.code_to_kor.items()}
        for line in [l.strip() for l in text.splitlines() if l.strip()]:
            if line.startswith("개선의견:"):
                improvement = line.replace("개선의견:", "").strip()
            elif line.startswith("과제검토구분코드:"):
                code = line.replace("과제검토구분코드:", "").strip()
            elif line.startswith("과제검토구분:"):
                val = line.replace("과제검토구분:", "").strip()
                code = kor_to_code.get(val, code)
        return improvement, code

    @staticmethod
    def parse_summary_response(text: str) -> str:
        """
        LLM 응답 텍스트에서 '문제분석:' 라인의 내용을 추출하여 요약 문자열로 반환합니다.
        """
        if not text:
            return ""
        for line in [l.strip() for l in text.splitlines() if l.strip()]:
            if line.startswith("문제분석:"):
                return line.replace("문제분석:", "").strip()
        return ""

    @staticmethod
    def normalize_refined_df(refined_df):
        """
        입력이 pandas DataFrame이면 레코드 리스트(dict 리스트)로 변환하고,
        그 외에는 원본을 그대로 반환합니다.
        """
        try:
            import pandas as pd
            if isinstance(refined_df, pd.DataFrame):
                return refined_df.to_dict(orient="records")
        except Exception:
            pass
        return refined_df

    @staticmethod
    def _get_row_value(row: Dict[str, Any], key: str, default: str = "") -> str:
        """
        행(dict)에서 특정 키의 값을 안전하게 문자열로 추출합니다.
        None 또는 'none'과 같은 값을 빈 문자열로 정규화합니다.
        """
        val = row.get(key, default) if isinstance(row, dict) else default
        return "" if val is None else str(val)

    def group_by_cust(self, refined_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[str]]]:
        """
        고객 식별자(qusnInvlTagtpUniqID) 기준으로 VOC, 의견, 코드 등 관련 필드를 그룹화합니다.
        반환 값은 고객별로 vocs, codes, opinions, cxc, proj, periods, timestamps 목록을 포함합니다.
        """
        kor_to_code = {v: k for k, v in self.code_to_kor.items()}
        groups: Dict[str, Dict[str, List[str]]] = {}
        for row in refined_rows:
            cust = self._get_row_value(row, "qusnInvlTagtpUniqID").strip()
            if not cust:
                continue

            voc = self._get_row_value(row, "voc").strip()
            cxc = self._get_row_value(row, "cxc").strip()
            code_raw = self._get_row_value(row, "과제검토구분").strip()
            opinion = self._get_row_value(row, "과제검토의견내용").strip()
            proj = self._get_row_value(row, "과제추진사업내용").strip()
            start = self._get_row_value(row, "개선이행시작년월일").strip()
            end = self._get_row_value(row, "개선이행종료년월일").strip()
            ts = self._get_row_value(row, "작성년월일시").strip()

            if cust not in groups:
                groups[cust] = {
                    "vocs": [], "codes": [], "opinions": [],
                    "cxc": [], "proj": [], "periods": [], "timestamps": []
                }

            if voc and voc.lower() != "none":
                groups[cust]["vocs"].append(voc)
            if cxc and cxc.lower() != "none":
                groups[cust]["cxc"].append(cxc)

            if code_raw:
                code = code_raw if code_raw in self.code_to_kor else kor_to_code.get(code_raw, "")
                if code:
                    groups[cust]["codes"].append(code)

            if opinion and opinion.lower() != "none":
                groups[cust]["opinions"].append(opinion)
            if proj and proj.lower() != "none":
                groups[cust]["proj"].append(proj)

            if start or end:
                pr = f"{start} ~ {end}".strip(" ~")
                groups[cust]["periods"].append(pr)

            if ts and ts.lower() != "none":
                groups[cust]["timestamps"].append(ts)

        return groups

    def build_existing_review_text(self, groups: Dict[str, Dict[str, List[str]]]) -> str:
        """
        그룹화된 데이터에서 기존 검토 의견, 작성일시, 개선과제, 과제기간을 모아
        사람이 읽기 쉬운 텍스트 블록으로 구성하여 반환합니다.
        """
        def clean(val: Any) -> str:
            s = str(val).strip() if val is not None else ""
            return "" if not s or s.lower() == "none" else s

        lines: List[str] = []
        for _, data in groups.items():
            opinions = [clean(x) for x in data.get("opinions", [])]
            timestamps = [clean(x) for x in data.get("timestamps", [])]
            projects = [clean(x) for x in data.get("proj", [])]
            periods = [clean(x) for x in data.get("periods", [])]

            n = max(len(opinions), len(timestamps), len(projects), len(periods)) or 0
            for i in range(n):
                entry: List[str] = [f"{i + 1}."]
                op = opinions[i] if i < len(opinions) else ""
                ts = timestamps[i] if i < len(timestamps) else ""
                pj = projects[i] if i < len(projects) else ""
                pr = periods[i] if i < len(periods) else ""
                if op:
                    entry.append(f"- 의견: {op}")
                if ts:
                    entry.append(f"- 작성일시: {ts}")
                if pj:
                    entry.append(f"- 개선과제: {pj}")
                if pr:
                    entry.append(f"- 과제기간: {pr}")
                if len(entry) > 1:
                    lines.append("\n".join(entry))
        return "\n".join(lines) if lines else ""

    async def build_suggestion_map_for_groups(self, groups: Dict[str, Dict[str, List[str]]], llm) -> Dict[str, str]:
        """
        고객별 기존 코드와 의견을 고려하여 제안 코드 맵을 생성합니다.
        - '02'(개선예정)가 있으면 우선 적용
        - '01'과 '03'이 동시에 있으면 LLM 판단으로 리스크 여부를 확인하여 결정
        - 그 외에는 단일 코드 또는 빈 값으로 설정
        """
        suggestion_map: Dict[str, str] = {}
        judge_template = load_prompt("judge_template.txt")

        for cust, data in groups.items():
            codes = set(data.get("codes", []))
            opinions_list = [o for o in data.get("opinions", []) if o and o.lower() != "none"]
            opinions_text = "- " + "\n- ".join(opinions_list) if opinions_list else "- (의견 없음)"

            if "02" in codes:
                suggestion_map[cust] = "02"
                continue

            if "01" in codes and "03" in codes:
                prompt = judge_template.format(opinions=opinions_text)
                resp = await llm.ainvoke([SystemMessage(content=prompt)])
                content = getattr(resp, "content", "") if resp else ""

                risk_exists = False
                for line in (content or "").splitlines():
                    line = line.strip()
                    if line.startswith("리스크존재여부:"):
                        risk_exists = line.replace("리스크존재여부:", "").strip().upper() == "YES"
                        break

                suggestion_map[cust] = "03" if risk_exists else "01"
            else:
                suggestion_map[cust] = next(iter(codes)) if len(codes) == 1 else ""

        return suggestion_map

    async def generate_llm_outputs_for_groups(
        self, groups: Dict[str, Dict[str, List[str]]], llm, suggestion_map: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        고객별 VOC와 의견을 LLM에 전달해 개선의견과 과제검토구분코드를 생성하고,
        기존 코드/의견 및 제안 맵을 반영하여 최종 코드와 한글명을 결정합니다.
        """
        prompt_template = load_prompt("suggestion_generate.txt")
        results: List[Dict[str, Any]] = []

        for cust, data in groups.items():
            vocs = [v for v in data.get("vocs", []) if v and v.lower() != "none"]
            opinions = [o for o in data.get("opinions", []) if o and o.lower() != "none"]

            voc_block = "- " + "\n- ".join(vocs) if vocs else "(VOC 없음)"
            opinions_block = "- " + "\n- ".join(opinions) if opinions else "- (의견 없음)"
            prompt = prompt_template.format(voc=voc_block, opinions=opinions_block)

            resp = await llm.ainvoke([SystemMessage(content=prompt)])
            content = getattr(resp, "content", "") if resp else ""

            improvement_opinion, task_code = self.parse_llm_response(content)

            existing_codes = set(data.get("codes", []))
            no_existing_opinion = not any([o for o in data.get("opinions", []) if o and o.lower() != "none"])
            no_existing_code = len(existing_codes) == 0

            if "02" in existing_codes:
                task_code = "02"
            else:
                if no_existing_code and no_existing_opinion:
                    task_code = task_code or suggestion_map.get(cust, "")
                else:
                    if suggestion_map.get(cust):
                        task_code = suggestion_map[cust] or task_code
                    elif len(existing_codes) == 1 and not task_code:
                        task_code = next(iter(existing_codes))

            kor_value = self.code_to_kor.get(task_code, "")
            results.append({
                "qusnInvlTagtpUniqID": cust,
                "voc_개선의견": improvement_opinion or "",
                "voc_과제검토구분코드": task_code or "",
                "voc_과제검토구분": kor_value or "",
            })

        return results

    async def build_summary_for_groups(self, groups: Dict[str, Dict[str, List[str]]], llm) -> Dict[str, str]:
        """
        고객별 CxC(고객 경험/클레임 등) 텍스트를 LLM에 전달해 문제분석 요약을 생성하고,
        '문제분석:' 라인을 파싱하여 맵으로 반환합니다.
        """
        summary_template = load_prompt("voc_summary.txt")
        summary_map: Dict[str, str] = {}

        for cust, data in groups.items():
            cx_list = [c for c in data.get("cxc", []) if c and c.lower() != "none"]
            cx_text = "- " + "\n- ".join(cx_list) if cx_list else "(정보 없음)"
            prompt = summary_template.format(cx=cx_text)
            resp = await llm.ainvoke([SystemMessage(content=prompt)])
            content = getattr(resp, "content", "") if resp else ""

            parsed_summary = self.parse_summary_response(content) or ""
            summary_map[cust] = parsed_summary

        return summary_map

    async def process(self, refined_df, llm) -> List[Dict[str, Any]]:
        """
        전체 파이프라인을 실행합니다.
        1) 입력 데이터 표준화 및 고객별 그룹화
        2) 제안 코드 맵 생성(LLM 포함 판단)
        3) LLM을 통해 개선의견/코드 생성 및 병합
        4) LLM 요약(문제분석) 생성
        5) 최종 리포트와 메타정보(qusnid, qsitmid 등) 구성하여 리스트로 반환
        """
        refined_rows = self.normalize_refined_df(refined_df)

        groups = self.group_by_cust(refined_rows)
        suggestion_map = await self.build_suggestion_map_for_groups(groups, llm)
        llm_rows = await self.generate_llm_outputs_for_groups(groups, llm, suggestion_map)
        summary_map = await self.build_summary_for_groups(groups, llm)

        cust_id_map: Dict[str, Dict[str, Any]] = {}
        for row in refined_rows:
            cust_key = (row.get("qusnInvlTagtpUniqID", "") or "").strip()
            if cust_key and cust_key not in cust_id_map:
                cust_id_map[cust_key] = {
                    "qusnid": row.get("qusnid"),
                    "qusnInvlTagtpUniqID": row.get("qusnInvlTagtpUniqID"),
                    "qsitmid": row.get("qsitmid", None),
                }

        results: List[Dict[str, Any]] = []
        kst = pytz.timezone("Asia/Seoul")
        kst_now = datetime.now(kst).strftime("%Y%m%d %H:%M:%S")

        for row in llm_rows:
            cust = row.get("qusnInvlTagtpUniqID", "") or ""
            direction = (row.get("voc_개선의견", "") or "").strip()

            existing_review_text = self.build_existing_review_text({cust: groups.get(cust, {})}).strip()
            if not existing_review_text:
                existing_review_text = "기존 검토 의견이 존재하지 않습니다"

            cust_group = groups.get(cust, {}) or {}
            proj_list = cust_group.get("proj", []) or []
            period_list = cust_group.get("periods", []) or []

            proj_display = (proj_list[0].strip() if proj_list and proj_list[0].strip() else "정보 없음")
            period_display = (period_list[0].strip() if period_list and period_list[0].strip() else "정보 없음")

            report = (
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "■ AI 검토 리포트\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "1) 기존 검토 의견\n"
                f"{existing_review_text}\n"
                f"- 개선과제 : {proj_display}\n"
                f"- 과제 기간 : {period_display}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "2) 검토 의견 제안 (문제 + 검토방향)\n"
                f"- 검토 의견 제안: {direction}\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            )

            suggestion_type_code = row.get("voc_과제검토구분코드") or suggestion_map.get(cust, "") or ""
            suggestion_type_kor = self.code_to_kor.get(suggestion_type_code, "")

            qusnid = row.get("qusnid")
            qusnInvlTagtpUniqID = row.get("qusnInvlTagtpUniqID") or cust
            qsitmid = row.get("qsitmid")

            if qusnid is None or qsitmid is None:
                fallback = cust_id_map.get(cust, {})
                qusnid = qusnid if qusnid is not None else fallback.get("qusnid")
                qsitmid = qsitmid if qsitmid is not None else fallback.get("qsitmid")

            results.append({
                "suggestionReport": report,
                "suggestionType": suggestion_type_kor,
                "ts": kst_now,
                "qusnid": qusnid,
                "qusnInvlTagtpUniqID": qusnInvlTagtpUniqID,
                "qsitmid": qsitmid,
            })

        return results

# 사용 예시
# gen = SuggestionGenerator()
# results = await gen.process(refined_df, llm)