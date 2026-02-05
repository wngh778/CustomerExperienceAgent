SELECT
      curr.채널명 AS 채널구분
    , curr.거래은행명 AS 거래은행구분
    , curr.고객경험단계명 AS 고객경험단계구분
    , curr.응답고객수
    , curr.NPS
    , prev.전반기NPS
    , curr.NPS - prev.전반기NPS AS HoH
FROM (
    SELECT
          채널명
        , 거래은행명
        , 고객경험단계명
        , 응답고객수
        , ROUND(NPS점수, 1) AS NPS
    FROM inst1.TSCCVMGC6 t1
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
        , 고객경험단계명
        , ROUND(NPS점수, 1) AS 전반기NPS
    FROM inst1.TSCCVMGC6 t2
    WHERE 1=1
      AND 거래은행명 != '시장평균'
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy_b1hf}'
      AND 반기구분명 = '{yyyyhf_b1hf}'
) prev
   ON curr.거래은행명 = prev.거래은행명
  AND curr.고객경험단계명 = prev.고객경험단계명