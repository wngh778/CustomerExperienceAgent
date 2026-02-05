WITH BANK AS (
    SELECT
          '전행' AS 지역영업그룹
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          - ROUND(SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          AS 전행NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND 지역영업그룹명 LIKE '%추진그룹' OR 지역영업그룹명 = '본부직할'
      AND 채널명 = '영업점'
    GROUP BY 1
)
SELECT
      REPLACE(지역영업그룹명, '·', '') AS 지역영업그룹
    ,   ROUND(SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
      - ROUND(SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
      AS 지역영업그룹NPS
    , (SELECT 전행NPS FROM BANK) AS 전행NPS
FROM inst1.TSCCVMGF3
WHERE 1=1
  AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
  AND 지역영업그룹명 LIKE '%추진그룹' OR 지역영업그룹명 = '본부직할'
  AND 채널명 = '영업점'
GROUP BY 1
ORDER BY 2 DESC