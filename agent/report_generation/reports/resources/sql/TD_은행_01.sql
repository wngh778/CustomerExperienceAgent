SELECT
      curr.조사년도
    , curr.반기구분명 AS 반기구분
    , curr.채널명 AS 채널구분
    , curr.거래은행명 AS 거래은행구분
    , curr.NPS
    , curr.NPS - prev.전반기NPS AS 전반기대비NPS
    , curr.NPS순위
    , COALESCE(prev.전반기NPS순위, 0) - COALESCE(curr.NPS순위, 0) AS 순위변동
FROM (
    SELECT
          조사년도
        , 반기구분명
        , 채널명
        , 거래은행명
        , ROUND(NPS점수, 1) AS NPS
        , RANK() OVER (
              PARTITION BY 조사년도, 반기구분명, 채널명
              ORDER BY NPS점수 DESC
          ) AS NPS순위
    FROM inst1.TSCCVMGC1 t1
    WHERE 1=1
      AND 거래은행명 != '시장평균'
      AND 조사년도 = '{yyyy}'
      AND 반기구분명 = '{yyyyhf}'
) curr
LEFT JOIN (
    SELECT
          채널명
        , 거래은행명
        , ROUND(NPS점수, 1) AS 전반기NPS
        , RANK() OVER (
              PARTITION BY 조사년도, 반기구분명, 채널명
              ORDER BY NPS점수 DESC
          ) AS 전반기NPS순위
    FROM inst1.TSCCVMGC1 t2
    WHERE 1=1
      AND 거래은행명 != '시장평균'
      AND 조사년도 = '{yyyy_b1hf}'
      AND 반기구분명 = '{yyyyhf_b1hf}'
) prev
   ON curr.채널명 = prev.채널명
  AND curr.거래은행명 = prev.거래은행명