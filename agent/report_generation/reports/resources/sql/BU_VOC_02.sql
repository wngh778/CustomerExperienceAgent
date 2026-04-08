WITH TMP AS (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , CCI점수 AS CCI
    FROM inst1.TSCCVMGE4
    WHERE 기준년월일 IN ('{friday_b01w}', '{friday_b02w}', '{endday_b01m}')
    UNION ALL
    SELECT
          기준년월일
        , '종합' AS 채널
        , ROUND(SUM(불만건수) * 100.0 / SUM(전체건수), 1) AS CCI
    FROM inst1.TSCCVMGE4
    WHERE 기준년월일 IN ('{friday_b01w}', '{friday_b02w}', '{endday_b01m}')
    GROUP BY 1, 2
)
SELECT
      t1.기준년월일
    , t1.채널
    , t1.CCI
    , t1.CCI - t2.CCI AS 전주대비CCI변동
    , t1.CCI - t3.CCI AS 전월대비CCI변동
FROM (
    SELECT *
    FROM TMP
    WHERE 기준년월일 = '{friday_b01w}'
) t1
LEFT JOIN (
    SELECT *
    FROM TMP
    WHERE 기준년월일 = '{friday_b02w}'
) t2
ON t1.채널 = t2.채널
LEFT JOIN (
    SELECT *
    FROM TMP
    WHERE 기준년월일 = '{endday_b01m}'
) t3
ON t1.채널 = t3.채널