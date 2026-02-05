SELECT A.설문ID,
A.설문조사방식명 AS 설문조사방식구분,
A.설문조사종류명 AS 설문조사종류구분,
A.조사년도,
A.반기구분명 AS 반기구분,
A.거래은행명 AS 거래은행구분,
A.채널명 AS 채널구분,
A.영향요인구분명 AS 영향요인구분,
A.문제영역명 AS 문제영역구분,
A.벤치마크은행명 AS 벤치마크은행구분,
ROUND(A.NPS중요도점수, 1) AS NPS중요도,
A.NPS중요도평균점수 AS NPS중요도평균,
A.NPS영향도점수 AS NPS영향도,
A.벤치마크NPS영향도점수 AS 벤치마크NPS영향도,
A.NPS영향도갭점수 AS NPS영향도GAP,
A.NPS영향도갭평균점수 AS NPS영향도GAP평균
FROM inst1.TSCCVMGC3 A
WHERE A.채널명 = '{{channel_type}}'	/* {{channel_type}}은행/브랜드/플랫폼/대면채널/고객센터/상품 */
AND A.조사년도 = '{{survey_year}}' /* {{survey_year}}조사년도 */
AND A.반기구분명 = '{{semester}}' /* {{semester}}상반기/하반기 */
;