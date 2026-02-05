from .logger import get_logger

import re

logger = get_logger(__name__)

def check_pii(text: str) -> bool:
    # 2.4 PII REGEX
    patterns = [
        (r"([A-Za-z0~9._%+-]+)@([A-Za-z0~9.-]+\.[A-Za-z]{2,})", "email"),
        (r"0\d{1,2}[- ]\d{3,4}[- ]\d{4}", "phone"),
        (r"010[- ]?\d{4}[- ]?\d{4}", "phone_2"),
        (r"^(.*\d+(?:-\d+)?)(?:\s+(.*))?$", "address"),
        (r"([0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))[-+ *. ]([5-8][0-9]{6})", "foreigner_id"),
        (r"\b\d{4}[- ]?(06|18)[- ]?\d{6}\b", "account_number_1"),
        (r"\b\d{4}(06|18)[- ]?\d{2}[- ]?\d{4}\b", "account_number_2"),
        (r"\b\d{6}[- ]?\d{2}[- ]?\d{6}\b", "account_number_3"),
        (r"\b\d{4}15[- ]?\d{2}[- ]?\d{5}\b", "account_number_4"),
        (r"[1-2][0-9]-[0-9]{2}-[0-9]{6}-[0-9]{2}", "driver_license"),
        (r"(010[-+ .*,]\d{4}[-+ .*,]\d{4}|8210[-+ .*,]\d{4}[-+ .*,]\d{4})", "phone_number_1"),
        (r"(^|\s)([DdMmRrSsGgTt])\d{8}(\s|$)", "passport_1"),
        (r"(^|\s)([DdMmRrSsGgTt])\d{3}[A-Z]\d{4}(\s|$)", "passport_2"),
        (r"(^|\s)[A-Z]{2}\d{7}(\s|$)", "passport_3"),
        (r"(?:9\d{3}|4\d{3}|5[1-5]\d{2}|6(?:011|5\d{2})|3[47]\d{2}|3(?:0[0-5]|[68]\d)|(?:2131|1800|35\d{3}))([- .]?\d{4}){3,4}", "credit_card"),
        (r"(?<![\dA-Za-z])\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])[\+\-\*\_][1-4]\d{6}(?![\dA-Za-z])", "resident_id")
    ]
    for pattern, name in patterns:
        match_result = re.search(pattern, text)
        if match_result:
            if name == "address" and match_result.group(2):
                return True
            if name != "address":
                return True
    return False

def mask_pii(text: str) -> bool:
    # 2.4 PII REGEX
    patterns = [
        (r"([A-Za-z0~9._%+-]+)@([A-Za-z0~9.-]+\.[A-Za-z]{2,})", "email"),
        (r"0\d{1,2}[- ]\d{3,4}[- ]\d{4}", "phone"),
        (r"010[- ]?\d{4}[- ]?\d{4}", "phone_2"),
        (r"([0-9]{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))[-+ *. ]([5-8][0-9]{6})", "foreigner_id"),
        (r"\b\d{4}[- ]?(06|18)[- ]?\d{6}\b", "account_number_1"),
        (r"\b\d{4}(06|18)[- ]?\d{2}[- ]?\d{4}\b", "account_number_2"),
        (r"\b\d{6}[- ]?\d{2}[- ]?\d{6}\b", "account_number_3"),
        (r"\b\d{4}15[- ]?\d{2}[- ]?\d{5}\b", "account_number_4"),
        (r"[1-2][0-9]-[0-9]{2}-[0-9]{6}-[0-9]{2}", "driver_license"),
        (r"(010[-+ .*,]\d{4}[-+ .*,]\d{4}|8210[-+ .*,]\d{4}[-+ .*,]\d{4})", "phone_number_1"),
        (r"(^|\s)([DdMmRrSsGgTt])\d{8}(\s|$)", "passport_1"),
        (r"(^|\s)([DdMmRrSsGgTt])\d{3}[A-Z]\d{4}(\s|$)", "passport_2"),
        (r"(^|\s)[A-Z]{2}\d{7}(\s|$)", "passport_3"),
        (r"(?:9\d{3}|4\d{3}|5[1-5]\d{2}|6(?:011|5\d{2})|3[47]\d{2}|3(?:0[0-5]|[68]\d)|(?:2131|1800|35\d{3}))([- .]?\d{4}){3,4}", "credit_card"),
        (r"(?<![\dA-Za-z])\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])[\+\-\*\_][1-4]\d{6}(?![\dA-Za-z])", "resident_id")
    ]

    mask_char = "*"
    def masking(m):
        logger.info(f"마스킹 대상 발견: {m.group()}")
        return mask_char * len(m.group())

    for pat, name in patterns:
        text = re.sub(pat, masking, text)

    return text
