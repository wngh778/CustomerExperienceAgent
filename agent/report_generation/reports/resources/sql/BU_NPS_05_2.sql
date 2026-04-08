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
    , t1.벤치마크은행명 AS 벤치마크은행구분
    , t1.문제영역명 AS 문제영역구분
    , t1.영향요인구분명 AS 영향요인구분
FROM inst1.TSCCVMGC3 t1
JOIN LATEST t2
   ON t1.조사년도 = t2.조사년도
  AND t1.반기구분명 = t2.반기구분명
WHERE 1=1
  AND t1.채널명 != '은행'
  AND t1.문제영역명 IN ('중점개선', '점진개선')