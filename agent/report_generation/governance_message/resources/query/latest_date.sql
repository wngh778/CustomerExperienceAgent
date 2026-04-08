-- base_date 기준 과거일자 중 tsccvmgc1, tsccvmgd5 기준으로 가장 최신 조사년도, 반기구분명, SELECT 조사년도, 반기구분명 as 반기구분,
SELECT 조사년도, 반기구분명 as 반기구분,
	(
      SELECT MAX(B.기준년월일)
      FROM inst1.TSCCVMGD5 B
      WHERE B.기준년월일 <= CAST('{base_date}' AS DATE)
    ) AS 기준년월일
FROM inst1.TSCCVMGC1
GROUP BY 1, 2
ORDER BY 1 DESC, 2 DESC
LIMIT 1;