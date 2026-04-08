SELECT
      curr.채널구분
    , curr.거래은행구분
    , curr.연령10세단위구분
    , curr.NPS
    , prev.전반기NPS
    , curr.NPS - prev.전반기NPS AS HoH
FROM (
    SELECT
          채널명 AS 채널구분
        , 거래은행명 AS 거래은행구분
        , 연령10세내용 AS 연령10세단위구분
        , ROUND(
              (SUM(CASE WHEN 추천의향내용 = '추천'   THEN 1 ELSE 0 END)
            -  SUM(CASE WHEN 추천의향내용 = '비추천' THEN 1 ELSE 0 END))
           * 100.0 / COUNT(1), 1) AS NPS
    FROM inst1.TSCCVMGF1
    WHERE 1=1
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy}'
      AND 반기구분명 = '{yyyyhf}'
      AND (고객경험단계명 = '' OR 채널명 = '상품') /* "상품"채널은 단독 추천의향 문항 부재 */
    GROUP BY 1, 2, 3
) curr
LEFT JOIN (
    SELECT
          채널명 AS 채널구분
        , 거래은행명 AS 거래은행구분
        , 연령10세내용 AS 연령10세단위구분
        , ROUND(
              (SUM(CASE WHEN 추천의향내용 = '추천'   THEN 1 ELSE 0 END)
            -  SUM(CASE WHEN 추천의향내용 = '비추천' THEN 1 ELSE 0 END))
           * 100.0 / COUNT(1), 1) AS 전반기NPS
    FROM inst1.TSCCVMGF1
    WHERE 1=1
      AND 채널명 = '{channel}'
      AND 조사년도 = '{yyyy_b1hf}'
      AND 반기구분명 = '{yyyyhf_b1hf}'
      AND (고객경험단계명 = '' OR 채널명 = '상품') /* "상품"채널은 단독 추천의향 문항 부재 */
    GROUP BY 1, 2, 3
) prev
   ON curr.거래은행구분 = prev.거래은행구분
  AND curr.연령10세단위구분 = prev.연령10세단위구분