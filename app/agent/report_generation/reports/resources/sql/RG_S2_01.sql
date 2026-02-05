WITH BANK AS (
    SELECT
          '전행' AS 지역영업그룹
        , ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '불만' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 전행CCI
    FROM inst1.TSCCVMGF4
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND 지역영업그룹명 LIKE '%지역영업그룹'
      AND 채널명 = '영업점'
    GROUP BY 1
)
SELECT
      REPLACE(지역영업그룹명, '·', '') AS 지역영업그룹
    , ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '불만' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 지역영업그룹CCI
    , (SELECT 전행CCI FROM BANK) AS 전행CCI
FROM inst1.TSCCVMGF4
WHERE 1=1
  AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
  AND 지역영업그룹명 LIKE '%지역영업그룹'
  AND 채널명 = '영업점'
GROUP BY 1
ORDER BY 2 DESC