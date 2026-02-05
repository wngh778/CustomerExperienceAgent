WITH LATEST AS (
    SELECT 조사년도, 반기구분명
    FROM inst1.TSCCVMGC1
    ORDER BY 조사년도 DESC, 반기구분명 DESC
    LIMIT 1
)
SELECT
      t1.조사년도
    , t1.반기구분명 AS 반기구분
    , t1.채널명 AS 채널구분
    , t1.거래은행명 AS 거래은행구분
    , ROUND(t1.NPS점수, 1) AS NPS
    , RANK() OVER (
          PARTITION BY t1.조사년도, t1.반기구분명, t1.채널명
          ORDER BY t1.NPS점수 DESC
      ) AS NPS순위
FROM inst1.TSCCVMGC1 t1
JOIN LATEST t2
   ON t1.조사년도 = t2.조사년도
  AND t1.반기구분명 = t2.반기구분명
WHERE 1=1
  AND t1.채널명 != '은행'
  AND t1.거래은행명 != '시장평균'