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
        , ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '칭찬' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 칭찬비율
        , ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '불만' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 불만비율
        , ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '불만' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS CCI
    FROM inst1.TSCCVMGF4
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
    GROUP BY 1
    ORDER BY 1
)
SELECT
      t1.기준년월
    , t2.칭찬비율
    , t2.불만비율
    , t2.CCI
FROM BASEYM t1
LEFT JOIN MONTHLY t2
ON t1.기준년월 = t2.기준년월