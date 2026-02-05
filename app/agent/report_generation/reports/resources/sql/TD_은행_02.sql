WITH CH_NPS AS (
    SELECT
          채널명
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
), CH_NPS_BM AS (
    SELECT all_bk.*
    FROM CH_NPS all_bk
    LEFT JOIN (
        SELECT
              채널명 AS 채널명_KB
            , NPS순위 - 1 AS NPS순위key
        FROM CH_NPS
        WHERE 거래은행명 = 'KB국민은행'
    ) kb
       ON all_bk.채널명 = kb.채널명_KB
    WHERE all_bk.거래은행명 = 'KB국민은행'
       OR all_bk.NPS순위 = kb.NPS순위key
), CH_NPS_FACTOR AS (
    SELECT
          curr.채널명
        , curr.거래은행명
        , curr.영향요인구분명
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
          AND 조사년도 = '{yyyy_b1hf}'
          AND 반기구분명 = '{yyyyhf_b1hf}'
    ) prev
       ON curr.채널명 = prev.채널명
      AND curr.거래은행명 = prev.거래은행명
      AND curr.영향요인구분명 = prev.영향요인구분명
)
SELECT
       ch.채널명 AS 채널구분
     , ft.영향요인구분명 AS 영향요인구분
     , MAX(CASE WHEN ch.거래은행명 != 'KB국민은행' THEN ch.거래은행명 ELSE -999 END) AS 벤치마크사
     , MAX(CASE WHEN ch.거래은행명 =  'KB국민은행' THEN ch.NPS순위      ELSE -999 END) AS KB순위
     , MAX(CASE WHEN ch.거래은행명 != 'KB국민은행' THEN ch.NPS순위      ELSE -999 END) AS 벤치마크사_순위
     , MAX(CASE WHEN ch.거래은행명 =  'KB국민은행' THEN ft.응답률       ELSE -999 END) AS 응답률
     , MAX(CASE WHEN ch.거래은행명 =  'KB국민은행' THEN ft.응답고객수   ELSE -999 END) AS 응답고객수
     , MAX(CASE WHEN ch.거래은행명 =  'KB국민은행' THEN ft.영향도       ELSE -999 END) AS 영향도
     , MAX(CASE WHEN ch.거래은행명 =  'KB국민은행' THEN ft.영향도변동   ELSE -999 END) AS 영향도변동
     , MAX(CASE WHEN ch.거래은행명 != 'KB국민은행' THEN ft.응답률       ELSE -999 END) AS 벤치마크사_응답률
     , MAX(CASE WHEN ch.거래은행명 != 'KB국민은행' THEN ft.응답고객수   ELSE -999 END) AS 벤치마크사_응답고객수
     , MAX(CASE WHEN ch.거래은행명 != 'KB국민은행' THEN ft.영향도       ELSE -999 END) AS 벤치마크사_영향도
     , MAX(CASE WHEN ch.거래은행명 != 'KB국민은행' THEN ft.영향도변동   ELSE -999 END) AS 벤치마크사_영향도변동
FROM CH_NPS_BM ch
LEFT JOIN CH_NPS_FACTOR ft
   ON ch.채널명 = ft.채널명
  AND ch.거래은행명 = ft.거래은행명
GROUP BY 1, 2