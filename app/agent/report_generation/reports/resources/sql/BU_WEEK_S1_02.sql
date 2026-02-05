SELECT
      SUM(설문발송건수) AS 총발송건수
    , SUM(응답접속수) AS 총응답접속수
    , SUM(응답완료수) AS 총응답완료수
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
      AND sv71.기준년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
      AND sv11.설문조사방식구분 = '02'
      AND sv11.설문조사종류구분 = '03'
) t