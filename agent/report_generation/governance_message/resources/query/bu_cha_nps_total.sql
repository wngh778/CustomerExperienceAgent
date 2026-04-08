select -- base_date 기준 최근 기준년월일 데이터 추출
	t.기준년월일,
	t.채널명 AS 채널,
	t.NPS점수 AS NPS,
	t.전체건수,
	t.추천건수,
	t.추천비율,
	t.중립건수,
	t.중립비율,
	t.비추천건수,
	t.비추천비율
from inst1.TSCCVMGD5 t
WHERE t.채널명 = '{{channel_type}}'
  AND t.기준년월일 = (
    SELECT tt.기준년월일
    FROM inst1.TSCCVMGD5 tt
    WHERE tt.채널명 = '{{channel_type}}'
      AND tt.기준년월일 <= '{{base_date}}'
    ORDER BY tt.기준년월일 DESC
    LIMIT 1
  )
UNION
select -- base_date 기준 전월 마지막 날짜 데이터 추출
	t.기준년월일,
	t.채널명 AS 채널,
	t.NPS점수 AS NPS,
	t.전체건수,
	t.추천건수,
	t.추천비율,
	t.중립건수,
	t.중립비율,
	t.비추천건수,
	t.비추천비율
from inst1.TSCCVMGD5 t
WHERE t.채널명 = '{{channel_type}}'
  AND t.기준년월일 = (
    SELECT tt.기준년월일
    FROM inst1.TSCCVMGD5 tt
    WHERE tt.채널명 = '{{channel_type}}'
      AND SUBSTRING(tt.기준년월일, 1, 6) = DATE_FORMAT(DATE_SUB(STR_TO_DATE('{{base_date}}', '%Y%m%d'), INTERVAL 1 MONTH), '%Y%m')
    ORDER BY tt.기준년월일 DESC
    LIMIT 1
  )
ORDER BY 기준년월일 DESC;