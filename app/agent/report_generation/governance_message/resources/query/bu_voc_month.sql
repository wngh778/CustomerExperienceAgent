SELECT 기준년월,
기준년월일,
채널명 AS 채널,
고객경험단계명 AS 고객경험단계,
서비스품질명 AS 서비스품질요소,
전체건수,
긍정건수,
부정건수,
중립건수,
칭찬건수,
불만건수,
개선건수,
기타건수,
긍정비율,
부정비율,
중립비율,
칭찬비율,
불만비율,
개선비율,
기타비율,
NSS점수 AS NSS,
CCI점수 AS CCI
FROM inst1.TSCCVMGE5
WHERE 기준년월 = '{{survey_year}}{{survey_month}}' /* {{survey_year}}{{survey_month}} 집계시점이 포함된 월 YYYYMM */
AND 채널명 = '{{channel_type}}' /* channel_type 채널명 */
AND (COALESCE('{{customer_experience_stage}}', '') = '' OR 고객경험단계명 = '{{customer_experience_stage}}')/* customer_experience_stage 고객경험단계명 */
;