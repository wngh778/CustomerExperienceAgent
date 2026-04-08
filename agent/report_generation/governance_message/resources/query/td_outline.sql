SELECT
    A.설문ID,
    A.설문조사방식명 AS 설문조사방식구분,
    A.설문조사종류명 AS 설문조사종류구분,
    A.조사년도,
    A.반기구분명 AS 반기구분,
    A.채널명 AS 채널구분,
    A.거래은행명 AS 거래은행구분,
    A.응답고객수 AS 응답자수,
    ROUND(A.남성응답자비율, 1) AS 남성응답자비율,
    ROUND(A.여성응답자비율, 1) AS 여성응답자비율,
    ROUND(A.연령20대응답자비율, 1) AS 연령20대응답자비율,
    ROUND(A.연령30대응답자비율, 1) AS 연령30대응답자비율,
    ROUND(A.연령40대응답자비율, 1) AS 연령40대응답자비율,
    ROUND(A.연령50대응답자비율, 1) AS 연령50대응답자비율,
    ROUND(A.연령60대응답자비율, 1) AS 연령60대응답자비율
FROM inst1.TSCCVMGC4 A
WHERE A.채널명 = '{{channel_type}}'	/* {{channel_type}}은행/브랜드/플랫폼/대면채널/고객센터/상품 */
AND A.조사년도 = '{{survey_year}}' /* {{survey_year}}조사년도 */
AND A.반기구분명 = '{{semester}}' /* {{semester}}상반기/하반기 */
;