SELECT
      t1.채널
    , t1.고객경험단계 
    , t1.NPS AS CXNPS
    , ROUND(t1.NPS - t2.NPS, 1) AS 전주대비CXNPS변동
FROM (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , REPLACE(고객경험단계명, ' NPS', '') AS 고객경험단계
        , NPS점수 AS NPS
    FROM inst1.TSCCVMGD7
    WHERE 기준년월일 = '{biz_endday_b01w}'
) t1
LEFT JOIN (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , REPLACE(고객경험단계명, ' NPS', '') AS 고객경험단계
        , NPS점수 AS NPS
    FROM inst1.TSCCVMGD7
    WHERE 기준년월일 = '{biz_endday_b02w}'
) t2
   ON t1.채널 = t2.채널
  AND t1.고객경험단계 = t2.고객경험단계
WHERE t1.고객경험단계 <> '직원'
ORDER BY
  CASE
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '로그인/인증' THEN 1
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '홈화면' THEN 2
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '계좌조회/이체' THEN 3
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '통합검색' THEN 4
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '금융상품몰' THEN 5
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '상품가입' THEN 6
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '상품관리/해지' THEN 7
    WHEN t1.채널 = 'KB 스타뱅킹' AND t1.고객경험단계 = '콘텐츠/서비스' THEN 8
    WHEN t1.채널 = '영업점'   AND t1.고객경험단계 = '내점/방문' THEN 9
    WHEN t1.채널 = '영업점'   AND t1.고객경험단계 = '대기' THEN 10
    WHEN t1.채널 = '영업점'   AND t1.고객경험단계 = '맞이/의도파악' THEN 11
    WHEN t1.채널 = '영업점'   AND t1.고객경험단계 = '직원상담' THEN 12
    WHEN t1.채널 = '영업점'   AND t1.고객경험단계 = '업무처리/배웅' THEN 13
    WHEN t1.채널 = '고객센터' AND t1.고객경험단계 = '버튼식ARS' THEN 14
    WHEN t1.채널 = '고객센터' AND t1.고객경험단계 = '보이는ARS' THEN 15
    WHEN t1.채널 = '고객센터' AND t1.고객경험단계 = '대기' THEN 16
    WHEN t1.채널 = '고객센터' AND t1.고객경험단계 = '챗봇상담' THEN 17
    WHEN t1.채널 = '고객센터' AND t1.고객경험단계 = '콜봇상담' THEN 18
    WHEN t1.채널 = '고객센터' AND t1.고객경험단계 = '직원상담' THEN 19
    WHEN t1.채널 = '상품'     AND t1.고객경험단계 = '저축성' THEN 20
    WHEN t1.채널 = '상품'     AND t1.고객경험단계 = '여신성' THEN 21
    WHEN t1.채널 = '상품'     AND t1.고객경험단계 = '투자성' THEN 22
    WHEN t1.채널 = '상품'     AND t1.고객경험단계 = '보장성' THEN 23
    WHEN t1.채널 = '상품'     AND t1.고객경험단계 = '외화환전' THEN 24
    WHEN t1.채널 = '상품'     AND t1.고객경험단계 = '해외송금' THEN 25
  END