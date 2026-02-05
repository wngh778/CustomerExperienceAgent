SELECT 기준년월,
기준년월일,
채널명 AS 채널,
고객경험단계명 AS 고객경험단계,
NPS점수 AS NPS,
전체건수,
추천건수,
추천비율,
중립건수,
중립비율,
비추천건수,
비추천비율
FROM inst1.TSCCVMGD7
WHERE 채널명 = '{{channel_type}}' /* channel_type 채널명 */
AND (COALESCE(REPLACE('{{customer_experience_stage}}', 'NPS', ''), '') = '' OR 고객경험단계명 = REPLACE('{{customer_experience_stage}}', 'NPS', ''))/* customer_experience_stage 고객경험단계명 */
;
