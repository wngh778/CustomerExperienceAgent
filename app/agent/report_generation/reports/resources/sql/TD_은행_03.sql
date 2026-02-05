WITH CCI AS (
    SELECT
          채널명
        , 거래은행명
        , 고객경험단계명
        , 응답고객수
        , 불만고객수
    FROM inst1.TSCCVMGD1 t1
    WHERE 1=1
      AND 조사년도 = '{yyyy}'
      AND 반기구분명 = '{yyyyhf}'
      AND 채널명 != '은행'
)
SELECT
      채널명 AS 채널구분
    , 거래은행명 AS 거래은행구분
    , 고객경험단계명 AS 고객경험단계구분
    , ROUND(불만고객수 * 100.0 / 응답고객수, 1) AS CCI
FROM CCI
UNION ALL
SELECT
      채널명 AS 채널구분
    , 거래은행명 AS 거래은행구분
    , '채널전체' AS 고객경험단계구분
    , ROUND(SUM(불만고객수) * 100.0 / SUM(응답고객수), 1) AS CCI
FROM CCI
GROUP BY 1, 2, 3