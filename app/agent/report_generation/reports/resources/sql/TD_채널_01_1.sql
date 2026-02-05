SELECT
      curr.조사년도
    , curr.반기구분명 AS 반기구분
    , curr.채널명 AS 채널구분
    , curr.거래은행명 AS 거래은행구분
    , curr.NPS
    , prev.전반기NPS
    , curr.추천비중
    , curr.중립비중
    , curr.비추천비중
    , prev.전반기추천비중
    , prev.전반기중립비중
    , prev.전반기비추천비중
FROM (
    SELECT
          조사년도
        , 반기구분명
        , 채널명
        , 거래은행명
        , ROUND(NPS점수, 1) AS NPS
        , ROUND(추천비중점수, 1) AS 추천비중
        , ROUND(중립비중점수, 1) AS 중립비중
        , ROUND(비추천비중점수, 1) AS 비추천비중
    FROM inst1.TSCCVMGC1 t1
    WHERE 1=1
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy}'
      AND 반기구분명 = '{yyyyhf}'
) curr
LEFT JOIN (
    SELECT
          채널명
        , 거래은행명
        , ROUND(NPS점수, 1) AS 전반기NPS
        , ROUND(추천비중점수, 1) AS 전반기추천비중
        , ROUND(중립비중점수, 1) AS 전반기중립비중
        , ROUND(비추천비중점수, 1) AS 전반기비추천비중
    FROM inst1.TSCCVMGC1 t2
    WHERE 1=1
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy_b1hf}'
      AND 반기구분명 = '{yyyyhf_b1hf}'
) prev
   ON curr.채널명 = prev.채널명
  AND curr.거래은행명 = prev.거래은행명