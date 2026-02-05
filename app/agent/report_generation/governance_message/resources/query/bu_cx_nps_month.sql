SELECT 기준년월,
기준년월일,
채널명 AS 채널,
고객경험단계명 AS 고객경험단계,
NPS점수 AS NPS,
전체건수,
추천건수,
ROUND(추천비율, 1) AS 추천비율,
중립건수,
ROUND(중립비율, 1) AS 중립비율,
비추천건수,
ROUND(비추천비율, 1) AS 비추천비율
FROM inst1.TSCCVMGD8
WHERE 기준년월 = '{{survey_year}}{{survey_month}}' /* {{survey_year}}{{survey_month}} 집계시점이 포함된 월 YYYYMM */
AND 채널명 = REPLACE('{{channel_type}}', 'KB ', '') /* channel_type 채널명 */
AND (COALESCE('{{customer_experience_stage}}', '') = '' OR 고객경험단계명 = '{{customer_experience_stage}}') /* customer_experience_stage 고객경험단계명 */
;