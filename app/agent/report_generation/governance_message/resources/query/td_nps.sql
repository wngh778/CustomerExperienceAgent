WITH CH_NPS AS (
    SELECT
          조사년도
        , 반기구분명
        , 채널명
        , 거래은행명
        , ROUND(NPS점수, 1) AS NPS
        , RANK() OVER (
              PARTITION BY 조사년도, 반기구분명, 채널명
              ORDER BY NPS점수 DESC
          ) AS NPS순위
    FROM inst1.TSCCVMGC1 t1
    WHERE 1=1
      AND 거래은행명 != '시장평균'
      AND 조사년도 = '{{survey_year}}'
      AND 반기구분명 = '{{semester}}'
),
BENCH AS (
    SELECT
          all_bk.조사년도
        , all_bk.반기구분명
        , all_bk.채널명
        , MAX(CASE WHEN all_bk.거래은행명 != 'KB국민은행' THEN all_bk.거래은행명 END) AS 벤치마크사
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
    GROUP BY 1, 2, 3
)
SELECT
    A.설문ID,
    A.설문조사방식명 AS 설문조사방식구분,
    A.설문조사종류명 AS 설문조사종류구분,
    A.조사년도,
    A.반기구분명 AS 반기구분,
    A.거래은행명 AS 거래은행구분,
    A.채널명 AS 채널구분,
    A.응답고객수,
    A.추천고객수,
    A.중립고객수,
    A.비추천고객수,
    ROUND(A.추천비중점수, 1) AS 추천비중,
    ROUND(A.중립비중점수, 1) AS 중립비중,
    ROUND(A.비추천비중점수, 1) AS 비추천비중,
    ROUND(A.NPS점수, 1) AS NPS점수,
    COALESCE(B.벤치마크사, '-') AS 벤치마크은행구분
FROM inst1.TSCCVMGC1 A
LEFT JOIN BENCH AS B
    ON A.채널명 = B.채널명
WHERE A.채널명 = '{{channel_type}}'	/* {{channel_type}} 은행/브랜드/플랫폼/대면채널/고객센터/상품 */
AND A.조사년도 = '{{survey_year}}' /* {{survey_year}} 조사년도 */
AND A.반기구분명 = '{{semester}}' /* {{semester}} 상반기/하반기 */
;