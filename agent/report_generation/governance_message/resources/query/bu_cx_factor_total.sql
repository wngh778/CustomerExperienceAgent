SELECT 
t.기준년월일,
t.채널명 AS 채널,
t.고객경험단계명 AS 고객경험단계,
t.서비스품질명 AS 서비스품질요소,
t.전체건수,
ROUND(t.추천비율, 1) AS 추천비율,
ROUND(t.중립비율, 1) AS 중립비율,
ROUND(t.비추천비율, 1) AS 비추천비율,
ROUND(t.영향도점수, 1) AS 영향도,
ROUND(t.전월영향도점수, 1) AS 전월영향도,
ROUND(t.전월대비영향도점수, 1) AS 전월대비영향도,
ROUND(t.전전월영향도점수, 1) AS 전전월영향도,
ROUND(t.전전월대비영향도점수, 1) AS 전전월대비영향도
FROM inst1.TSCCVMGE2 t
WHERE 채널명 = '{{channel_type}}' /* channel_type 채널명 */
AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR 고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
AND t.기준년월일 = (
    SELECT tt.기준년월일
    FROM inst1.TSCCVMGE2 tt
    WHERE tt.채널명 = '{{channel_type}}'
      AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR tt.고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
      AND tt.기준년월일 <= '{{base_date}}'
    ORDER BY tt.기준년월일 DESC
    LIMIT 1
  )
;
