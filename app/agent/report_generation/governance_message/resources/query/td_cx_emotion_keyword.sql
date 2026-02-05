SELECT 
    A.설문ID,
    A.설문조사방식명 AS 설문조사방식구분,
    A.설문조사종류명 AS 설문조사종류구분,
    A.조사년도,
    A.반기구분명 AS 반기구분,
    A.거래은행명 AS 거래은행구분,
    A.채널명 AS 채널구분,
    A.고객경험단계명 AS 고객경험단계구분,
    A.VOC유형명 AS VOC유형구분,
    A.서비스품질명 AS 서비스품질요소명,
    A.고객접점명 AS 고객접점용어명,
    A.키워드수 AS 언급량
FROM inst1.TSCCVMGD2 A
WHERE A.채널명 = '{{channel_type}}'	/* {{channel_type}}은행/브랜드/플랫폼/대면채널/고객센터/상품 */
AND A.조사년도 = '{{survey_year}}' /* {{survey_year}}조사년도 */
AND A.반기구분명 = '{{semester}}' /* {{semester}}상반기/하반기 */
AND A.고객경험단계명 = '{{customer_experience_stage}}' /* {{customer_experience_stage}}채널 단위 */
;