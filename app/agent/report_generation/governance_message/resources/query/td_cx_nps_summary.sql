SELECT 
    설문ID,
    설문조사방식명 AS 설문조사방식구분,
    설문조사종류명 AS 설문조사종류구분,
    조사년도,
    반기구분명 AS 반기구분,
    거래은행명 AS 거래은행구분,
    채널명 AS 채널구분,
    고객경험단계명 AS 고객경험단계구분,
    NPS점수,
    벤치마크은행명 AS 벤치마크은행구분,
    벤치마크NPS점수,
    벤치마크NPS점수갭점수 as 벤치마크NPS점수갭
FROM inst1.TSCCVMGC5
WHERE 채널명 = '{{channel_type}}'
AND 조사년도 = '{{survey_year}}'
AND 반기구분명 = '{{semester}}'
;