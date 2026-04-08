SELECT t.기준년월, -- base_date 기준 최근 기준년월일 데이터 추출
       t.기준년월일,
       t.채널명 AS 채널,
       t.고객경험단계명 AS 고객경험단계,
       t.NPS점수 AS NPS,
       t.전체건수,
       t.추천건수,
       t.추천비율,
       t.중립건수,
       t.중립비율,
       t.비추천건수,
       t.비추천비율
FROM inst1.TSCCVMGD7 t
WHERE t.채널명 = '{{channel_type}}'
  AND (COALESCE(REPLACE('{{customer_experience_stage}}', 'NPS', ''), '') = '' OR t.고객경험단계명 = REPLACE('{{customer_experience_stage}}', 'NPS', ''))
  AND t.기준년월일 = (
    SELECT tt.기준년월일
    FROM inst1.TSCCVMGD7 tt
    WHERE tt.채널명 = '{{channel_type}}'
      AND (COALESCE(REPLACE('{{customer_experience_stage}}', 'NPS', ''), '') = '' OR tt.고객경험단계명 = REPLACE('{{customer_experience_stage}}', 'NPS', ''))
      AND tt.기준년월일 <= '{{base_date}}'
    ORDER BY tt.기준년월일 DESC
    LIMIT 1
  )
UNION
SELECT t.기준년월, -- base_date 기준 전월 마지막 날짜 데이터 추출
       t.기준년월일,
       t.채널명 AS 채널,
       t.고객경험단계명 AS 고객경험단계,
       t.NPS점수 AS NPS,
       t.전체건수,
       t.추천건수,
       t.추천비율,
       t.중립건수,
       t.중립비율,
       t.비추천건수,
       t.비추천비율
FROM inst1.TSCCVMGD7 t
WHERE t.채널명 = '{{channel_type}}'
  AND (COALESCE(REPLACE('{{customer_experience_stage}}', 'NPS', ''), '') = '' OR t.고객경험단계명 = REPLACE('{{customer_experience_stage}}', 'NPS', ''))
  AND t.기준년월일 = (
    SELECT tt.기준년월일
    FROM inst1.TSCCVMGD7 tt
    WHERE tt.채널명 = '{{channel_type}}'
      AND (COALESCE(REPLACE('{{customer_experience_stage}}', 'NPS', ''), '') = '' OR tt.고객경험단계명 = REPLACE('{{customer_experience_stage}}', 'NPS', ''))
      AND tt.기준년월 = DATE_FORMAT(DATE_SUB(STR_TO_DATE('{{base_date}}', '%Y%m%d'), INTERVAL 1 MONTH), '%Y%m')
    ORDER BY tt.기준년월일 DESC
    LIMIT 1
  )
ORDER BY 기준년월일 DESC;