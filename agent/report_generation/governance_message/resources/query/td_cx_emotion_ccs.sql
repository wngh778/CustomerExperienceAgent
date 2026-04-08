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
              채널명 AS 채널구분_KB
            , NPS순위 - 1 AS NPS순위key
        FROM CH_NPS
        WHERE 거래은행명 = 'KB국민은행'
    ) kb
       ON all_bk.채널명 = kb.채널구분_KB
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
    A.고객경험단계명 AS 고객경험단계구분,
    A.응답고객수,
    A.칭찬고객수,
    A.불만고객수,
    A.개선고객수,
    A.기타고객수,
    ROUND(A.칭찬비중점수, 1) AS 칭찬비중,
    ROUND(A.불만비중점수, 1) AS 불만비중,
    ROUND(A.개선비중점수, 1) AS 개선비중,
    ROUND(A.기타비중점수, 1) AS 기타비중,
    ROUND(A.CCI점수, 1) AS CCS점수,
    COALESCE(B.벤치마크사, '-') AS 벤치마크은행구분
FROM inst1.TSCCVMGD1 A
LEFT JOIN BENCH AS B
    ON A.채널명 = B.채널명
WHERE A.채널명 = '{{channel_type}}'	/* {{channel_type}}은행/브랜드/플랫폼/대면채널/고객센터/상품 */
AND A.조사년도 = '{{survey_year}}' /* {{survey_year}}조사년도 */
AND A.반기구분명 = '{{semester}}' /* {{semester}}상반기/하반기 */
AND A.고객경험단계명 = '{{customer_experience_stage}}' /* {{customer_experience_stage}}채널 단위 */
;