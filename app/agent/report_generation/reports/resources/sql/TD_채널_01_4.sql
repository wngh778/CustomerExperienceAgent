SELECT
      curr.채널명 as 채널구분
    , curr.거래은행명 AS 거래은행구분
    , curr.영향요인구분명 AS 영향요인구분
    , curr.응답률
    , curr.응답고객수
    , curr.영향도
    , curr.영향도 - prev.전반기영향도 AS 영향도변동
FROM (
    SELECT
          채널명
        , 거래은행명
        , 영향요인구분명
        , 응답고객수
        , ROUND(전체대비응답비중점수, 1) AS 응답률
        , ROUND(NPS영향도점수, 1) AS 영향도
    FROM inst1.TSCCVMGC2 t1
    WHERE 1=1
      AND 거래은행명 != '시장평균'
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy}'
      AND 반기구분명 = '{yyyyhf}'
) curr
LEFT JOIN (
    SELECT
          채널명
        , 거래은행명
        , 영향요인구분명
        , ROUND(NPS영향도점수, 1) AS 전반기영향도
    FROM inst1.TSCCVMGC2 t2
    WHERE 1=1
      AND 거래은행명 != '시장평균'
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy_b1hf}'
      AND 반기구분명 = '{yyyyhf_b1hf}'
) prev
   ON curr.채널명 = prev.채널명
  AND curr.거래은행명 = prev.거래은행명
  AND curr.영향요인구분명 = prev.영향요인구분명