SELECT 기준년월일,
채널명 AS 채널,
고객경험단계명 AS 고객경험단계,
서비스품질명 AS 서비스품질요소,
전체건수,
ROUND(추천비율, 1) AS 추천비율,
ROUND(중립비율, 1) AS 중립비율,
ROUND(비추천비율, 1) AS 비추천비율,
ROUND(영향도점수, 1) AS 영향도,
ROUND(전월영향도점수, 1) AS 전월영향도,
ROUND(전월대비영향도점수, 1) AS 전월대비영향도,
ROUND(전전월영향도점수, 1) AS 전전월영향도,
ROUND(전전월대비영향도점수, 1) AS 전전월대비영향도
FROM inst1.TSCCVMGE2
WHERE 채널명 = '{{channel_type}}' /* channel_type 채널명 */
AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR 고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
;