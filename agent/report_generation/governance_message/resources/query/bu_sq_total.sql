with today as (
  SELECT t.기준년월일, t.고객경험단계명, t.서비스품질명, t.전체건수, 
    round(t.추천비율 * d9.전체건수 / 100, 0) as 추천건수,
    round(t.비추천비율 * d9.전체건수 / 100, 0) as 비추천건수
  FROM inst1.TSCCVMGE2 t
  left join inst1.tsccvmgd9 d9 on t.기준년월일 = d9.기준년월일 and t.채널명 = d9.채널명 and t.고객경험단계명 = replace(d9.고객경험단계명, ' ', '')
  WHERE t.채널명 = '{{channel_type}}' /* channel_type 채널명 */
  AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR t.고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
  AND t.기준년월일 = (
      SELECT tt.기준년월일
      FROM inst1.TSCCVMGE2 tt
      WHERE tt.채널명 = '{{channel_type}}'
        AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR tt.고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
        AND tt.기준년월일 <= '{{base_date}}'
      ORDER BY tt.기준년월일 DESC
      LIMIT 1
  )
),
last_month as ( -- 전월
  SELECT t.기준년월일, t.고객경험단계명, t.서비스품질명, t.전체건수, 
    round(t.추천비율 * d9.전체건수 / 100, 0) as 추천건수,
    round(t.비추천비율 * d9.전체건수 / 100, 0) as 비추천건수
  FROM inst1.TSCCVMGE2 t
  left join inst1.tsccvmgd9 d9 on t.기준년월일 = d9.기준년월일 and t.채널명 = d9.채널명 and t.고객경험단계명 = replace(d9.고객경험단계명, ' ', '')
  WHERE t.채널명 = '{{channel_type}}' /* channel_type 채널명 */
  AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR t.고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
  AND t.기준년월일 = (
      SELECT tt.기준년월일
      FROM inst1.TSCCVMGE2 tt
      where 기준년월일 >= DATE_FORMAT(STR_TO_DATE('{{base_date}}', '%Y%m%d'), '%Y0101') 
      and substring(tt.기준년월일, 1, 6) = DATE_FORMAT(DATE_SUB(STR_TO_DATE('{{base_date}}', '%Y%m%d'), INTERVAL 1 MONTH), '%Y%m')
      order by tt.기준년월일 desc
      limit 1
  )
),
two_last_month as ( -- 전전월
  SELECT t.기준년월일, t.고객경험단계명, t.서비스품질명, t.전체건수, 
    round(t.추천비율 * d9.전체건수 / 100, 0) as 추천건수,
    round(t.비추천비율 * d9.전체건수 / 100, 0) as 비추천건수
  FROM inst1.TSCCVMGE2 t
  left join inst1.tsccvmgd9 d9 on t.기준년월일 = d9.기준년월일 and t.채널명 = d9.채널명 and t.고객경험단계명 = replace(d9.고객경험단계명, ' ', '')
  WHERE t.채널명 = '{{channel_type}}' /* channel_type 채널명 */
  AND (COALESCE('{{factor_customer_experience_stage}}', '') = '' OR t.고객경험단계명 = '{{factor_customer_experience_stage}}')/*factor_customer_experience_stage 고객경험단계명 */
  AND t.기준년월일 = (
      SELECT tt.기준년월일
      FROM inst1.TSCCVMGE2 tt
      where 기준년월일 >= DATE_FORMAT(STR_TO_DATE('{{base_date}}', '%Y%m%d'), '%Y0101') 
      and substring(tt.기준년월일, 1, 6) = DATE_FORMAT(DATE_SUB(STR_TO_DATE('{{base_date}}', '%Y%m%d'), INTERVAL 2 MONTH), '%Y%m')
      order by tt.기준년월일 desc
      limit 1
  )
)
SELECT t.기준년월일, t.고객경험단계명, t.서비스품질명,
round((t.추천건수 - l.추천건수) / (greatest(t.전체건수 - l.전체건수, 1)) * 100, 1) as 월추천비율, -- 분모가 0이하면 1로
round((t.비추천건수 - l.비추천건수) / (greatest(t.전체건수 - l.전체건수, 1)) * 100, 1) as 월비추천비율,
round((t.추천건수 - l.추천건수) / (greatest(t.전체건수 - l.전체건수, 1)) * 100 - (t.비추천건수 - l.비추천건수) / (greatest(t.전체건수 - l.전체건수, 1)) * 100, 1) as 월누적NPS,
round((t.추천건수 - ll.추천건수) / (greatest(t.전체건수 - ll.전체건수, 1)) * 100, 1) as 전월추천비율,
round((t.비추천건수 - ll.비추천건수) / (greatest(t.전체건수 - ll.전체건수, 1)) * 100, 1) as 전월비추천비율,
round((t.추천건수 - ll.추천건수) / (greatest(t.전체건수 - ll.전체건수, 1)) * 100 - (t.비추천건수 - ll.비추천건수) / (greatest(t.전체건수 - ll.전체건수, 1)) * 100, 1) as 전월누적NPS
from today t
join last_month l on t.고객경험단계명 = l.고객경험단계명 and t.서비스품질명 = l.서비스품질명
join two_last_month ll on t.고객경험단계명 = ll.고객경험단계명 and t.서비스품질명 = ll.서비스품질명
;