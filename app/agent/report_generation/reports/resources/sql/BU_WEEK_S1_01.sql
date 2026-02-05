WITH TMP AS (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , NPS점수 AS NPS
    FROM inst1.TSCCVMGD5
    WHERE 기준년월일 IN ('{biz_endday_b01w}', '{biz_endday_b02w}', '{biz_endday_b01m}')
    UNION ALL
    SELECT
          기준년월일
        , '종합' AS 채널
        , ROUND(SUM(추천건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(비추천건수) * 100.0 / SUM(전체건수), 1) AS NPS
    FROM inst1.TSCCVMGD5
    WHERE 기준년월일 IN ('{biz_endday_b01w}', '{biz_endday_b02w}', '{biz_endday_b01m}')
    GROUP BY 1, 2
)
SELECT
      t1.채널
    , t1.NPS
    , t1.NPS - t2.NPS AS 전주대비NPS변동
    , t1.NPS - t3.NPS AS 전월대비NPS변동
FROM (
    SELECT *
    FROM TMP
    WHERE 기준년월일 = '{biz_endday_b01w}'
) t1
LEFT JOIN (
    SELECT *
    FROM TMP
    WHERE 기준년월일 = '{biz_endday_b02w}'
) t2
ON t1.채널 = t2.채널
LEFT JOIN (
    SELECT *
    FROM TMP
    WHERE 기준년월일 = '{biz_endday_b01m}'
) t3
ON t1.채널 = t3.채널
ORDER BY
  CASE t1.채널
    WHEN '종합' THEN 1
    WHEN 'KB 스타뱅킹' THEN 2
    WHEN '영업점' THEN 3
    WHEN '고객센터' THEN 4
    WHEN '상품' THEN 5
  END