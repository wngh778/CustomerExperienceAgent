from collections import defaultdict
from typing import Literal
from pydantic import BaseModel, Field
from langchain_core.output_parsers import PydanticOutputParser


# =============================================================================
# Pydantic Output Models
# =============================================================================
from pydantic import BaseModel, Field
from typing import List

class CXElement(BaseModel):
    """발굴된 개별 고객경험요소"""
    element_name: str = Field(
        description="고객경험요소명 (고유명사/구체적 서비스·기능명)"
    )
    parent_factor_name: str = Field(
        description="매핑되는 상위 서비스품질요소명"
    )
    relevant_voc_id_list: list[int] = Field(
        description="해당 신규 고객경험요소와 관련된 VOC ID 리스트"
    )
    reasoning: str = Field(
        description="이 요소를 신규 고객경험요소로 판단한 근거 (1-2문장)"
    )

class CXDiscoveryResult(BaseModel):
    """고객경험요소 발굴 결과"""
    summary: str = Field(
        description="고객경험요소 발굴 결과 요약"
    )
    discovered_elements: list[CXElement] = Field(
        description="새로 발굴된 고객경험요소 목록"
    )

class MergedCXElement(BaseModel):
    """여러 고객경험요소들 중 의미적으로 유사한 것들을 병합하여 추출한 고객경험요소"""
    element_name: str = Field(
        description="고객경험요소명 (고유명사/구체적 서비스·기능명)"
    )
    parent_factor_name: str = Field(
        description="매핑되는 상위 서비스품질요소명"
    )
    relevant_discovered_cxe_id_list: list[int] = Field(
        description="해당 고객경험요소를 결정하는데 관련이 있었던 신규 추출 고객경험요소의 id 목록"
    )
    reasoning: str = Field(
        description="이 요소를 신규 고객경험요소로써 추가해야하는 근거 (1-2문장)"
    )
    integrate_reasoning: str = Field(
        description="여러 개의 고객경험요소를 이 요소로 통합하기로 결정한 근거 (1-2문장)"
    )

class CXMergeResult(BaseModel):
    """여러 배치별로 추출된 신규 고객경험요소를 병합한 결과"""
    summary: str = Field(
        description="고객경험요소 종합 결과 요약"
    )
    discovered_elements: list[MergedCXElement] = Field(
        description="새로 발굴된 고객경험요소들을 종합해서 정제하여 추출한 신규 고객경험요소 목록"
    )


# =============================================================================
# Utils
# =============================================================================
async def get_channel_code(channel_name, mcp_executor):
    query = f"""SELECT 설문조사대상구분 AS 채널구분
FROM (
SELECT DISTINCT 설문조사대상구분 
FROM INST1.TSCCVCI18 
WHERE 설문조사방식구분='02' AND 설문조사종류구분='03'
) A
JOIN INST1.TSCCVCI04 N1 ON N1.인스턴스내용='{channel_name}' AND N1.그룹회사코드="KB0" AND A.설문조사대상구분=N1.인스턴스코드 AND N1.인스턴스식별자='142447000' """
    res = await mcp_executor.execute_tool("mysql_query", {"query": query})

    if len(res) == 1:
        return res[0]['채널구분']
    elif len(res) > 1:
        raise ValueError(f"Duplicated channel names : {res}")
    else:
        raise ValueError(f"No Channel Name : {channel_name}")

async def get_cx_stage_code(channel_code, cx_stage_name, mcp_executor):
    query = f"""SELECT 고객경험단계구분
FROM (
SELECT DISTINCT 고객경험단계구분 
FROM INST1.TSCCVCI18 
WHERE 설문조사방식구분='02' AND 설문조사종류구분='03' AND 설문조사대상구분='{channel_code}'
) A
JOIN INST1.TSCCVCI04 N1 ON N1.인스턴스내용='{cx_stage_name}' AND N1.그룹회사코드="KB0" AND A.고객경험단계구분=N1.인스턴스코드 AND N1.인스턴스식별자='142594000' """

    res = await mcp_executor.execute_tool("mysql_query", {"query": query})

    if len(res) == 1:
        return res[0]['고객경험단계구분']
    elif len(res) > 1:
        raise ValueError(f"Duplicated cx_stage names : {res}")
    else:
        raise ValueError(f"No Cx Stage Name : {cx_stage_name}")
        
    return res

# =============================================================================
# System Prompt
# =============================================================================

CX_ELEMENT_DISCOVERY_SYSTEM = """<agent_config>
role: cx_element_discoverer
reasoning_effort: medium
verbosity: low
</agent_config>

<context>
You are discovering NEW customer experience elements (고객경험요소) from VOC (Voice of Customer) data.

The CX hierarchy is:
  Channel(채널) → Experience Stage(고객경험단계) → Quality Factor(서비스품질요소) → **Customer Experience Element(고객경험요소)**

Customer Experience Elements are the MOST GRANULAR level.
They represent specific products, services, features, or touchpoints that customers actually interact with and mention by name.

You are given:
1. A batch of VOC texts from a specific channel and experience stage
2. Existing quality factors and known CX elements for context
3. Product/service terms (상품서비스용어) and performance/quality terms (성능품질용어) already extracted from these VOCs by an external system
</context>

<task>
Analyze the VOC texts and discover NEW customer experience elements that:
1. Are NOT already in the existing CX elements list
2. Are SPECIFIC enough to be actionable
3. Represent newly introduced features, services, or touchpoints

For each discovered element:
- Map it to the most appropriate parent quality factor from the provided list (Do not change the name of parent quality factor)

Available Parent Quality Factors:
{sq} 

- Return the list of VOC IDs that serve as the basis for discovering that element
</task>

<specificity_rules>
추출 대상:
- 고객이 VOC에서 고유명사로 지칭한 특정 상품, 서비스, 기능명의 주요 특성
- 최근 신규 도입되어 기존 품질요소 체계에 아직 반영되지 않은 구체적 기능/서비스의 특성
- 고객이 특정 화면, 프로세스, 인증수단 등을 명시적으로 언급한 경우

제외 대상:
- 일반적 동작/상태 표현 (서비스 개선, 불편함 해소, 시스템 오류 등)
- 감정/평가 표현 (고객 불만, 만족, 불편 등)
- 구체적 대상이 특정되지 않은 포괄적 표현
</specificity_rules>

<deduplication_rules>
- Check against existing CX elements provided in <existing_cx_elements>
- Check against product/service terms (상품서비스용어) provided in <product_service_terms>
- If an element is essentially the same as an existing one but with slightly different wording, do NOT include it
- If an element overlaps with a quality_factor name (parent level), do NOT include it
- Focus on elements that are genuinely NEW — features or services recently introduced or not yet captured in the existing hierarchy
</deduplication_rules>

<output_format>
Output JSON matching the schema. No explanation outside the JSON structure.
{output_format_text}
</output_format>"""


# =============================================================================
# Merge CXEs System Prompt
# =============================================================================

CX_ELEMENT_DISCOVERY_USER = """<input>
<filter_context>
<channel>{channel_name}</channel>
<experience_stage>{stage_name}</experience_stage>
</filter_context>

<existing_hierarchy>
<existing_cx_elements>
{existing_cx_elements_text}
</existing_cx_elements>
</existing_hierarchy>


<voc_data>
{voc_batch_text}
</voc_data>
</input>

Discover NEW customer experience elements from the VOC data above.
Focus on SPECIFIC, NAMED features/products/services. Exclude generic terms."""

MERGE_CXE_SYSTEM = """<agent_config>
role: cx_element_discoverer
reasoning_effort: medium
verbosity: low
</agent_config>

<context>
You are discovering NEW customer experience elements (고객경험요소) from VOC (Voice of Customer) data.

The CX hierarchy is:
  Channel(채널) → Experience Stage(고객경험단계) → Quality Factor(서비스품질요소) → **Customer Experience Element(고객경험요소)**

Customer Experience Elements are the MOST GRANULAR level.
They represent specific products, services, features, or touchpoints that customers actually interact with and mention by name.

You are given:
1. Newly discovered customer experience elements (고객경험요소) from VOC (Voice of Customer) data.
</context>

<task>
Generate the refined result for the given discovered customer experience elements.
<instruction>
- Integrate the meaningly similar customer experience elements in one integrated and refine customer experience element.

For each discovered element:
- Map it to the most appropriate parent quality factor from the provided list (Do not change the name of parent quality factor)

Available Parent Quality Factors:
{sq}

- Provide the list of original customer‑experience‑element IDs that make up the integrated customer experience element.
</instruction>
</task>

<output_format>
Output JSON matching the schema. No explanation outside the JSON structure.
{output_format_text}
</output_format>
"""
# =============================================================================
# Merge CXEs User Prompt
# =============================================================================

MERGE_CXE_USER = """<input>
<discovered_cx_elements>
{discovered_cxe_text}
</discovered_cx_elements>
</input>
"""

# =============================================================================
# Query Template
# =============================================================================

CXE_QUERY_TEMPLATE = """SELECT 
	N2.서비스품질요소명 AS 서비스품질요소
	, 고객경험요소명
	, 고객경험요소내용
FROM INST1.TSCCVCI18 A
LEFT JOIN (
	SELECT DISTINCT 설문조사방식구분, 설문조사종류구분, 설문조사대상구분, 서비스품질요소코드, 서비스품질요소명 
	FROM (
		SELECT 설문조사방식구분, 설문조사종류구분, 서비스품질요소코드, 서비스품질요소명,
			CASE WHEN 고객경험단계구분 IN ('07','08','09','10','11','12') THEN "09" ELSE 설문조사대상구분 
			END AS 설문조사대상구분
		FROM INST1.TSCCVCI07
		WHERE 사용여부=1) B
) N2 ON A.설문조사방식구분=N2.설문조사방식구분 AND A.설문조사대상구분=N2.설문조사대상구분 AND A.서비스품질요소코드=N2.서비스품질요소코드
WHERE A.설문조사방식구분='02' AND A.설문조사종류구분='03' AND A.설문조사대상구분='{channel_code}' AND A.고객경험단계구분='{cx_stage_code}'; """

VOC_QUERY_TEMPLATE = """SELECT
    SV50.설문응답종료년월일, 
    SV50.설문ID, 
    SV50.문항ID, 
    SV50.설문참여대상자고유ID, 
    SV50.상품서비스용어내용, 
    SV50.성능품질용어내용, 
    SV73.문항응답내용 AS VOC원문 
    FROM (
        SELECT * FROM INST1.TSCCVSV50
        WHERE 고객경험요소분류성공여부=0 AND 상품서비스용어내용<>"" AND 성능품질용어내용<>""
            AND 설문조사방식구분='02' AND 설문조사종류구분='03' AND VOC필터링여부=0
            AND 텍스트제외사유내용="" AND 부점장제공여부=1 AND 직원제공여부=1
            AND 문항설문조사대상구분='{channel_code}' AND 고객경험단계구분='{cx_stage_code}'
            AND 설문응답종료년월일 BETWEEN '{start_date}' AND '{end_date}'
    ) SV50
    LEFT JOIN (
        SELECT * FROM INST1.TSCCVSV73
        WHERE 그룹회사코드='KB0' AND 문항선택항목ID=""
            AND 설문조사방식구분='02' AND 설문조사종류구분='03' AND 문항구분='01'
            AND IF(고객경험단계구분 IN ('07','08','09','10','11','12'), 고객경험단계구분='{cx_stage_code}', 설문조사대상구분='{channel_code}' AND 고객경험단계구분='{cx_stage_code}')
            AND 설문응답종료년월일 BETWEEN '{start_date}' AND '{end_date}'
    ) SV73 ON SV50.설문ID=SV73.설문ID AND SV50.설문참여대상자고유ID=SV73.설문참여대상자고유ID AND SV50.문항ID=SV73.문항ID """

def _split_into_batches(items: list, batch_size: int) -> list[list]:
    """리스트를 batch_size 단위로 분할"""
    voc_ids = list(range(len(items)))
    id_batch = [voc_ids[i:i + batch_size] for i in range(0, len(items), batch_size)]
    item_batch = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    return id_batch, item_batch

def format_voc_batch(voc_ids: list[int], vocs: list[dict]) -> str:
    """VOC 배치를 프롬프트용 텍스트로 포맷

    각 VOC는 classification_id, voc_text, factor_name, voc_type, keywords를 포함.
    """
    if not vocs:
        return "없음"
    lines = ["ID | 상품서비스용어내용 | 성능품질용어내용 | VOC원문"]
    for id_num, v in zip(voc_ids, vocs):
        text = v.get("VOC원문", "").strip()
        service_name = v.get("상품서비스용어내용", "")
        quality_name = v.get("성능품질용어내용", "")
        line = f"{id_num} | {service_name} | {quality_name} | {text}"
        lines.append(line)
    return "\n".join(lines)

def format_existing_cx_elements(elements: list[dict]) -> str:
    """기존 고객경험요소 목록을 프롬프트용 텍스트로 포맷"""
    if not elements:
        return "없음 (아직 추출된 고객경험요소가 없습니다)"
    lines = ["서비스품질요소 | 고객경험요소 | 고객경험요소내용"]
    for e in elements:
        lines.append(
            f"{e['서비스품질요소']} | {e['고객경험요소명']} | {e['고객경험요소내용']} "
        )
    return "\n".join(lines)

def format_discovered_cx_elements(elements: list[dict]) -> str:
    """신규 발굴 고객경험요소 목록"""
    if not elements:
        return "없음 (신규 발굴된 고객경험요소가 없습니다)"
    lines = ["ID | 고객경험요소 | 고객경험요소발굴근거"]
    for id_num, e in enumerate(elements):
        lines.append(
            f"{id_num} | {e.element_name} | {e.reasoning}"
        )
    return "\n".join(lines)

def format_existing_sq(elements: list[dict]) -> str:
    """매핑할 서비스 품질요소들의 목록"""
    return ",".join(list(set([e['서비스품질요소'] for e in elements])))


def format_cxe_discover_messages(channel, cx_stage, sq, cxe, voc_batch_text):
    existing_cx_elements_text = format_existing_cx_elements(cxe)
    parser = PydanticOutputParser(pydantic_object=CXDiscoveryResult)
    output_format_text = parser.get_format_instructions()
    
    return [
        {"role": "system", "content": CX_ELEMENT_DISCOVERY_SYSTEM.format(
            sq=sq,
            output_format_text=output_format_text,
        )},
        {
            "role": "user",
            "content": CX_ELEMENT_DISCOVERY_USER.format(
                channel_name=channel,
                stage_name=cx_stage,
                existing_cx_elements_text=existing_cx_elements_text,
                voc_batch_text=voc_batch_text
            ),
        },
    ]

    
def format_cxe_merge_messages(channel, cx_stage, sq, discovered_cxe):
    discovered_cxe_text = format_discovered_cx_elements(discovered_cxe)
    parser = PydanticOutputParser(pydantic_object=CXMergeResult)
    output_format_text = parser.get_format_instructions()

    return [
        {"role": "system", "content": MERGE_CXE_SYSTEM.format(
            sq=sq,
            output_format_text=output_format_text,
        )},
        {
            "role": "user",
            "content": MERGE_CXE_USER.format(
                discovered_cxe_text=discovered_cxe_text,
            ),
        },
    ]