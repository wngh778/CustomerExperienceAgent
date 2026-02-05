WITH TMP AS (
    SELECT
          기준년월
        , 고객경험단계명
        , 전체건수
        , ROUND(전체건수 * (추천비율/100), 0) AS 추천건수
        , ROUND(전체건수 * (비추천비율/100), 0) AS 비추천건수
        , ROUND(영향도점수, 1) AS NPS영향도
    FROM inst1.TSCCVMGE1
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
      AND 채널명 = '{channel}'
), NPS_IMP AS (
    SELECT
          기준년월
        , 고객경험단계명
        , ROUND(SUM(추천건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(비추천건수) * 100.0 / SUM(전체건수), 1) AS NPS영향도
    FROM TMP
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '누적' AS 기준년월
        , 고객경험단계명
        , ROUND(SUM(추천건수) * 100.0 / SUM(전체건수), 1) - ROUND(SUM(비추천건수) * 100.0 / SUM(전체건수), 1) AS NPS영향도
    FROM TMP
    GROUP BY 1, 2
)
SELECT
      고객경험단계명 AS 영향요인
    , MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN NPS영향도 ELSE NULL END) AS '1월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}02' THEN NPS영향도 ELSE NULL END) AS '2월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}03' THEN NPS영향도 ELSE NULL END) AS '3월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}04' THEN NPS영향도 ELSE NULL END) AS '4월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}05' THEN NPS영향도 ELSE NULL END) AS '5월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}06' THEN NPS영향도 ELSE NULL END) AS '6월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}07' THEN NPS영향도 ELSE NULL END) AS '7월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}08' THEN NPS영향도 ELSE NULL END) AS '8월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}09' THEN NPS영향도 ELSE NULL END) AS '9월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}10' THEN NPS영향도 ELSE NULL END) AS '10월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}11' THEN NPS영향도 ELSE NULL END) AS '11월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '{yyyy}12' THEN NPS영향도 ELSE NULL END) AS '12월NPS영향도'
    , MAX(CASE WHEN 기준년월 = '누적' THEN NPS영향도 ELSE NULL END) AS '누적NPS영향도'
    , ROUND(MAX(CASE WHEN 기준년월 = '누적' THEN NPS영향도 ELSE NULL END)
          - MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN NPS영향도 ELSE NULL END), 1) AS '1월대비NPS영향도차이'
FROM NPS_IMP
GROUP BY 1
ORDER BY
  CASE
    WHEN 고객경험단계명 = '로그인/인증' THEN 1
    WHEN 고객경험단계명 = '홈화면' THEN 2
    WHEN 고객경험단계명 = '계좌조회/이체' THEN 3
    WHEN 고객경험단계명 = '통합검색' THEN 4
    WHEN 고객경험단계명 = '금융상품몰' THEN 5
    WHEN 고객경험단계명 = '상품가입' THEN 6
    WHEN 고객경험단계명 = '상품관리/해지' THEN 7
    WHEN 고객경험단계명 = '콘텐츠/서비스' THEN 8
    WHEN 고객경험단계명 = '내점/방문' THEN 9
    WHEN 고객경험단계명 = '대기' THEN 10
    WHEN 고객경험단계명 = '맞이/의도파악' THEN 11
    WHEN 고객경험단계명 = '상담' THEN 12
    WHEN 고객경험단계명 = '업무처리/배웅' THEN 13
    WHEN 고객경험단계명 = '버튼식ARS' THEN 14
    WHEN 고객경험단계명 = '보이는ARS' THEN 15
    WHEN 고객경험단계명 = '대기' THEN 16
    WHEN 고객경험단계명 = '챗봇상담' THEN 17
    WHEN 고객경험단계명 = '콜봇상담' THEN 18
    WHEN 고객경험단계명 = '직원상담' THEN 19
    WHEN 고객경험단계명 = '저축성' THEN 20
    WHEN 고객경험단계명 = '여신성' THEN 21
    WHEN 고객경험단계명 = '투자성' THEN 22
    WHEN 고객경험단계명 = '보장성' THEN 23
    WHEN 고객경험단계명 = '외화환전' THEN 24
    WHEN 고객경험단계명 = '해외송금' THEN 25
  END