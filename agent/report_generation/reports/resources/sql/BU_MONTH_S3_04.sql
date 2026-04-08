WITH TMP AS (
    SELECT
          기준년월
        , 고객경험단계명
        , 전체건수
        , 비추천비율
        , 채널명
    FROM inst1.TSCCVMGE1
    WHERE 기준년월 BETWEEN '{yyyy01}' AND '{yyyymm_b01m}'
      AND 채널명 = '{channel}'
), DTT_RTO AS (
    SELECT
          기준년월
        , 고객경험단계명
        , 비추천비율
        , 채널명
    FROM TMP
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '누적' AS 기준년월
        , 고객경험단계명
        , 비추천비율
        , 채널명
    FROM TMP
    GROUP BY 1, 2
)
SELECT
      고객경험단계명 AS 고객경험단계
    , MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN round(비추천비율,1) ELSE NULL END) AS '1월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}02' THEN round(비추천비율,1) ELSE NULL END) AS '2월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}03' THEN round(비추천비율,1) ELSE NULL END) AS '3월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}04' THEN round(비추천비율,1) ELSE NULL END) AS '4월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}05' THEN round(비추천비율,1) ELSE NULL END) AS '5월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}06' THEN round(비추천비율,1) ELSE NULL END) AS '6월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}07' THEN round(비추천비율,1) ELSE NULL END) AS '7월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}08' THEN round(비추천비율,1) ELSE NULL END) AS '8월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}09' THEN round(비추천비율,1) ELSE NULL END) AS '9월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}10' THEN round(비추천비율,1) ELSE NULL END) AS '10월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}11' THEN round(비추천비율,1) ELSE NULL END) AS '11월비추천비율'
    , MAX(CASE WHEN 기준년월 = '{yyyy}12' THEN round(비추천비율,1) ELSE NULL END) AS '12월비추천비율'
    , MAX(CASE WHEN 기준년월 = '누적' THEN round(비추천비율,1) ELSE NULL END) AS '누적비추천비율'
    , ROUND(MAX(CASE WHEN 기준년월 = '누적' THEN round(비추천비율,1) ELSE NULL END)
          - MAX(CASE WHEN 기준년월 = '{yyyy}01' THEN round(비추천비율,1) ELSE NULL END), 1) AS '1월대비비추천비율차이'
FROM DTT_RTO
GROUP BY 1
ORDER BY
  CASE
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '로그인/인증' THEN 1
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '홈화면' THEN 2
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '계좌조회/이체' THEN 3
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '통합검색' THEN 4
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '금융상품몰' THEN 5
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '상품가입' THEN 6
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '상품관리/해지' THEN 7
    WHEN 채널명 = '스타뱅킹' AND 고객경험단계 = '콘텐츠/서비스' THEN 8
    WHEN 채널명 = '영업점' AND 고객경험단계 = '내점/방문' THEN 9
    WHEN 채널명 = '영업점' AND 고객경험단계 = '대기' THEN 10
    WHEN 채널명 = '영업점' AND 고객경험단계 = '맞이/의도파악' THEN 11
    WHEN 채널명 = '영업점' AND 고객경험단계 = '직원상담' THEN 12
    WHEN 채널명 = '영업점' AND 고객경험단계 = '업무처리/배웅' THEN 13
    WHEN 채널명 = '고객센터' AND 고객경험단계 = '버튼식ARS' THEN 14
    WHEN 채널명 = '고객센터' AND 고객경험단계 = '보이는ARS' THEN 15
    WHEN 채널명 = '고객센터' AND 고객경험단계 = '대기' THEN 16
    WHEN 채널명 = '고객센터' AND 고객경험단계 = '챗봇상담' THEN 17
    WHEN 채널명 = '고객센터' AND 고객경험단계 = '콜봇상담' THEN 18
    WHEN 채널명 = '고객센터' AND 고객경험단계 = '직원상담' THEN 19
    WHEN 채널명 = '상품' AND 고객경험단계 = '저축성' THEN 20
    WHEN 채널명 = '상품' AND 고객경험단계 = '여신성' THEN 21
    WHEN 채널명 = '상품' AND 고객경험단계 = '투자성' THEN 22
    WHEN 채널명 = '상품' AND 고객경험단계 = '보장성' THEN 23
    WHEN 채널명 = '상품' AND 고객경험단계 = '외화환전' THEN 24
    WHEN 채널명 = '상품' AND 고객경험단계 = '해외송금' THEN 25
  END