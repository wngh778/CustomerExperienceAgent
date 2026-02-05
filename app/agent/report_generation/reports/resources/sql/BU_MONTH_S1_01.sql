SELECT
      SUM(설문발송건수) AS 발송건수
    , SUM(응답접속수) AS 응답접촉
    , ROUND(SUM(응답접속수) * 100.0 / SUM(설문발송건수), 2) AS 접촉률
    , SUM(응답완료수) AS 응답완료
    , ROUND(SUM(응답완료수) * 100.0 / SUM(설문발송건수), 2) AS 응답률
FROM (
    SELECT
          sv71.설문ID
        , sv71.기준년월일
        , sv71.설문발송건수
        , sv71.응답접속수
        , sv71.응답완료수
        , sv11.설문조사방식구분
        , sv11.설문조사종류구분
        , sv11.설문조사대상구분
    FROM inst1.TSCCVSV71 sv71
    LEFT JOIN inst1.TSCCVSV11 sv11
       ON sv71.설문ID = sv11.설문ID
    WHERE 1=1
      AND sv71.기준년월일 BETWEEN '{yyyy0101}' AND '{yyyymmdd_b01m}'
      AND sv11.설문조사방식구분 = '02'
      AND sv11.설문조사종류구분 = '03'
) t