WITH MONTHLY AS (
    SELECT
          기준년월
        , TRIM(지역본부명) AS 지역본부
        , 고객경험단계명
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          - ROUND(SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          AS NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
      AND 고객경험단계명 IN ('채널', '직원')
    GROUP BY 1, 2, 3
), NOW AS (
    SELECT
          기준년월
        , TRIM(지역본부명) AS 지역본부
        , 고객경험단계명
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          - ROUND(SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          AS NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
      AND 고객경험단계명 IN ('채널', '직원')
    GROUP BY 1, 2, 3
), LAST_2W AS (
    SELECT
          기준년월
        , TRIM(지역본부명) AS 지역본부
        , 고객경험단계명
        ,   ROUND(SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          - ROUND(SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1)
          AS NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b03w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
      AND 고객경험단계명 IN ('채널', '직원')
    GROUP BY 1, 2, 3
)
SELECT
      REPLACE(t1.지역본부, '지역본부', '') AS 지역본부
    , t1.고객경험단계명 AS 고객경험단계
    , MAX(t3.NPS) AS '2주전PGNPS'
    , MAX(t2.NPS) AS '금주PGNPS'
    , MAX(t3.NPS) - MAX(t2.NPS) AS 'PGNPS변동'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}01' THEN t1.NPS ELSE NULL END) AS '1월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}02' THEN t1.NPS ELSE NULL END) AS '2월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}03' THEN t1.NPS ELSE NULL END) AS '3월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}04' THEN t1.NPS ELSE NULL END) AS '4월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}05' THEN t1.NPS ELSE NULL END) AS '5월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}06' THEN t1.NPS ELSE NULL END) AS '6월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}07' THEN t1.NPS ELSE NULL END) AS '7월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}08' THEN t1.NPS ELSE NULL END) AS '8월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}09' THEN t1.NPS ELSE NULL END) AS '9월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}10' THEN t1.NPS ELSE NULL END) AS '10월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}11' THEN t1.NPS ELSE NULL END) AS '11월PGNPS'
    , MAX(CASE WHEN t1.기준년월 = '{yyyy}12' THEN t1.NPS ELSE NULL END) AS '12월PGNPS'
FROM MONTHLY t1
LEFT JOIN NOW t2
   ON t1.지역본부 = t2.지역본부
  AND t1.고객경험단계명 = t2.고객경험단계명
LEFT JOIN LAST_2W t3
   ON t1.지역본부 = t3.지역본부
  AND t1.고객경험단계명 = t3.고객경험단계명
GROUP BY 1, 2
ORDER BY
      CAST(REGEXP_SUBSTR(SUBSTRING_INDEX(t1.지역본부, '(', 1), '[0-9]+') AS UNSIGNED)
    , t1.지역본부
    , CASE WHEN t1.고객경험단계명 = '채널' THEN 1 ELSE 2 END