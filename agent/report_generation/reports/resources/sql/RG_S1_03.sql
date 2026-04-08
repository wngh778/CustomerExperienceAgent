WITH BASEYM AS (
    SELECT '{yyyy}01' AS 기준년월
    UNION ALL
    SELECT '{yyyy}02' AS 기준년월
    UNION ALL
    SELECT '{yyyy}03' AS 기준년월
    UNION ALL
    SELECT '{yyyy}04' AS 기준년월
    UNION ALL
    SELECT '{yyyy}05' AS 기준년월
    UNION ALL
    SELECT '{yyyy}06' AS 기준년월
    UNION ALL
    SELECT '{yyyy}07' AS 기준년월
    UNION ALL
    SELECT '{yyyy}08' AS 기준년월
    UNION ALL
    SELECT '{yyyy}09' AS 기준년월
    UNION ALL
    SELECT '{yyyy}10' AS 기준년월
    UNION ALL
    SELECT '{yyyy}11' AS 기준년월
    UNION ALL
    SELECT '{yyyy}12' AS 기준년월
), MONTHLY AS (
    SELECT
          기준년월
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 추천비율
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (7, 8)                THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 중립비율
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 비추천비율
        ,   ROUND((SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END)
          - SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END)) * 100.0 / COUNT(1), 1)
          AS NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점' AND 고객경험단계명 = '해당무'
    GROUP BY 1
    ORDER BY 1
)
SELECT
      t1.기준년월
    , t2.추천비율
    , t2.중립비율
    , t2.비추천비율
    , t2.NPS
FROM BASEYM t1
LEFT JOIN MONTHLY t2
ON t1.기준년월 = t2.기준년월