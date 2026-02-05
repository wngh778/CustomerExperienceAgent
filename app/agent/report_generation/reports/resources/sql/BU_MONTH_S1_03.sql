WITH TMP AS (
    SELECT
          기준년월
        , 채널명 AS 채널
        , ROUND(SUM(긍정건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(부정건수) * 100.0 / SUM(전체건수), 1) AS NSS
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          기준년월
        , '종합' AS 채널
        , ROUND(SUM(긍정건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(부정건수) * 100.0 / SUM(전체건수), 1) AS NSS
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '누적' AS 기준년월
        , 채널명 AS 채널
        , ROUND(SUM(긍정건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(부정건수) * 100.0 / SUM(전체건수), 1) AS NSS
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '누적' AS 기준년월
        , '종합' AS 채널
        , ROUND(SUM(긍정건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(부정건수) * 100.0 / SUM(전체건수), 1) AS NSS
    FROM inst1.TSCCVMGE5
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2
)
SELECT
      채널
    , MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN NSS ELSE NULL END) AS '1월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}02' THEN NSS ELSE NULL END) AS '2월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}03' THEN NSS ELSE NULL END) AS '3월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}04' THEN NSS ELSE NULL END) AS '4월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}05' THEN NSS ELSE NULL END) AS '5월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}06' THEN NSS ELSE NULL END) AS '6월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}07' THEN NSS ELSE NULL END) AS '7월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}08' THEN NSS ELSE NULL END) AS '8월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}09' THEN NSS ELSE NULL END) AS '9월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}10' THEN NSS ELSE NULL END) AS '10월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}11' THEN NSS ELSE NULL END) AS '11월NSS'
    , MAX(CASE WHEN 기준년월 = '{yyyy}12' THEN NSS ELSE NULL END) AS '12월NSS'
    , MAX(CASE WHEN 기준년월 = '누적' THEN NSS ELSE NULL END) AS '누적NSS'
    , ROUND(MAX(CASE WHEN 기준년월 = '누적' THEN NSS ELSE NULL END)
          - MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN NSS ELSE NULL END), 1) AS '1월대비NSS차이'
FROM TMP
GROUP BY 1
ORDER BY
  CASE 채널
    WHEN '종합' THEN 1
    WHEN '스타뱅킹' THEN 2
    WHEN '영업점' THEN 3
    WHEN '고객센터' THEN 4
    WHEN '상품' THEN 5
  END