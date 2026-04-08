from enum import Enum

##################################################
##                  CommonCode                  ##
##################################################
class CommonCode(Enum):

    # LLM 응답 결과 KEY값
    RESPONSE_KEY="response"
    RESULT_KEY="result"
    
    # 해당무 고객경험단계 코드
    NO_CX_STAGE_CD="00"
    
    # 채널별 고객경험요소 미분류 ID값
    NO_CXE_ID="9999"

    # CXE Dict Keys
    CXE_NAME="cxe_nm"
    CXE_DESC="cxe_desc"
    SQ_CD="sq_cd"
    SQ_NAME="sq_nm"

    # 이전 실행 용어 셋 keys
    PREV_WORD_SET_DICT_KEY="{chnl_cd}_{cxe_cd}"

    # 부정 감정 대분류 코드
    NEGATIVE_EMOTION_LARGE_CD = "02"


##################################################
##                   Messages                   ##
##################################################
class CxeFailedMessage(Enum):
    UNKNOWN_CXE="존재하지 않는 고객경험요소"
    NOT_CLASSIFIED="분류되지 않음"

class OfferExcludeMessage(Enum):
    WORD_LENGTH="20음절이하"
    EXCEPT_WORD_INCLUDE="불용어포함"

class VocFilterMessage(Enum):
    DUPLICATE_PK="중복된 PK"
    TEXT_FILTER="일정 음절 이하(자모, 반복문자, 특수문자, 공백 제거 후)"
    CH_STGE_NOT_RELATED="채널/고객경험단계와 무관함"


class EmotionAnalysisMessage(Enum):
    UNKNOWN_EMOTION="존재하지 않는 감정중분류"
    LLM_EMPTY_RESPONSE="감정분석 실패(빈 LLM 생성 응답)"

class EntityWordDetectMessage(Enum):
    LLM_EMPTY_RESPONSE="개체어 식별 실패(빈 LLM 생성 응답)"