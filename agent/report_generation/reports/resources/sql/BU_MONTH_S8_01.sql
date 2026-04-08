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
    UNION ALL
    SELECT '누적' AS 기준년월
), MONTHLY AS (
    SELECT
          기준년월
        , '종합' AS 채널
        , ROUND(SUM(불만건수) * 100.0 / SUM(전체건수), 1) AS CCI
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '누적' AS 기준년월
        , '종합' AS 채널
        , ROUND(SUM(불만건수) * 100.0 / SUM(전체건수), 1) AS CCI
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
), CUM AS (
    SELECT
          '누적' AS 기준년월
        , '종합' AS 채널
        , ROUND(SUM(불만건수) * 100.0 / SUM(전체건수), 1) AS 누적CCI
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
)
SELECT
      t1.기준년월
    , t2.CCI
    , t3.누적CCI
FROM BASEYM t1
LEFT JOIN MONTHLY t2
ON t1.기준년월 = t2.기준년월
LEFT JOIN CUM t3
ON t2.채널 = t3.채널