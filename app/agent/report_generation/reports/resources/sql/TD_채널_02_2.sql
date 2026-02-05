SELECT
      채널명 AS 채널구분
    , 거래은행명 AS 거래은행구분
    , 고객경험단계명 AS 고객경험단계구분
    , 영향요인구분명 AS 영향요인구분
    , ROUND(전체대비응답비중점수, 1) AS 응답률
    , ROUND(NPS영향도점수, 1) AS NPS영향도
FROM inst1.TSCCVMGC7
WHERE 1=1
  AND 거래은행명 != '시장평균'
  AND 채널명 = '{channel}'
  AND 조사년도 = '{yyyy}'
  AND 반기구분명 = '{yyyyhf}'
  AND 영향요인구분명 != '고객경험단계전체'