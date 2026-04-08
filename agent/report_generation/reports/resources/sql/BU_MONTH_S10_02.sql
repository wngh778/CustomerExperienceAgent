WITH BASE AS (
    SELECT
          기준년월
        , 채널명
        , 고객경험단계명
        , NPS점수 AS NPS
    FROM inst1.TSCCVMGD8
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
), CUM AS (
    SELECT
          '누적' AS 기준년월
        , 채널명
        , 고객경험단계명
        , ROUND(SUM(추천건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(비추천건수) * 100.0 / SUM(전체건수), 1) AS NPS
    FROM inst1.TSCCVMGD8
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2, 3
)
SELECT
      REPLACE(채널명, '스타뱅킹', 'KB 스타뱅킹') AS _채널
    , case -- MGD8을 만들때 사용되는 MGB5 테이블에 영업점-상담으로 되어있음 (26년 기준 직원상담)
        when 채널명 = '영업점' and REPLACE(고객경험단계명, ' NPS', '') = '상담' then '직원상담'
        else REPLACE(고객경험단계명, ' NPS', '')
      end AS _고객경험단계
    , MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN NPS ELSE NULL END) AS '1월고객경험단계NPS'
    , MAX(CASE WHEN 기준년월 = '누적' THEN NPS ELSE NULL END) AS 누적고객경험단계NPS
    , ROUND(MAX(CASE WHEN 기준년월 = '누적' THEN NPS ELSE NULL END)
          - MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN NPS ELSE NULL END), 1) AS '1월대비고객경험단계NPS차이'
FROM (
    SELECT *
    FROM BASE
    UNION ALL
    SELECT *
    FROM CUM
) t
GROUP BY 1, 2