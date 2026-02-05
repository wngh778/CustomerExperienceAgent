SELECT A.설문ID,
A.설문조사방식명 AS 설문조사방식구분,
A.설문조사종류명 AS 설문조사종류구분,
A.조사년도,
A.반기구분명 AS 반기구분,
A.거래은행명 AS 거래은행구분,
A.채널명 AS 채널구분,
A.고객경험단계명 AS 고객경험단계구분,
A.응답고객수,
A.추천고객수,
A.중립고객수,
A.비추천고객수,
ROUND(A.추천비중점수, 1) AS 추천비중,
ROUND(A.중립비중점수, 1) AS 중립비중,
ROUND(A.비추천비중점수, 1) AS 비추천비중,
ROUND(A.NPS점수, 1) AS NPS점수
FROM inst1.TSCCVMGC6 A
WHERE A.채널명 = '{{channel_type}}'	/* {{channel_type}}은행/브랜드/플랫폼/대면채널/고객센터/상품 */
AND A.조사년도 = '{{survey_year}}' /* {{survey_year}}조사년도 */
AND A.반기구분명 = '{{semester}}' /* {{semester}}상반기/하반기 */
AND A.고객경험단계명 = '{{customer_experience_stage}}' /* {{customer_experience_stage}}채널 단위 */
;