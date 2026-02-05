WITH TMP AS (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , ROUND(SUM(긍정건수) * 100.0 / SUM(전체건수) - SUM(부정건수) * 100.0 / SUM(전체건수), 1) AS CHNSS
        , ROUND(SUM(긍정건수) * 100.0 / SUM(전체건수), 1) AS CH긍정비율
        , ROUND(SUM(부정건수) * 100.0 / SUM(전체건수), 1) AS CH부정비율
        , ROUND(SUM(불만건수) * 100.0 / SUM(전체건수), 1) AS CHCCI
        , ROUND(SUM(칭찬건수) * 100.0 / SUM(전체건수), 1) AS CH칭찬비율
        , ROUND(SUM(불만건수) * 100.0 / SUM(전체건수), 1) AS CH불만비율
    FROM inst1.TSCCVMGE4
    WHERE 기준년월일 IN ('{biz_endday_b01w}', '{biz_endday_b02w}', '{biz_endday_b01m}')
    GROUP BY 1, 2
)
SELECT
      t1.채널
    , t1.CHNSS
    , ROUND(t1.CHNSS - t2.CHNSS, 1) AS 전주대비CHNSS변동
    , t1.CH긍정비율
    , t1.CH부정비율
    , t1.CHCCI
    , ROUND(t1.CHCCI - t2.CHCCI, 1) AS 전주대비CHCCI변동
    , t1.CH칭찬비율
    , t1.CH불만비율
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
  CASE 
    WHEN t1.채널 = 'KB 스타뱅킹' THEN 1
    WHEN t1.채널 = '영업점' THEN 2
    WHEN t1.채널 = '고객센터' THEN 3
    WHEN t1.채널 = '상품' THEN 4
  END