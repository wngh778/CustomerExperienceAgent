SELECT
      t1.기준년월일
    , t1.채널
    , REPLACE(t1.고객경험단계, ' NPS', '') AS 고객경험단계
    , t1.NPS
    , t1.NPS - t2.NPS AS 전주대비NPS변동
FROM (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , 고객경험단계명 AS 고객경험단계
        , NPS점수 AS NPS
    FROM inst1.TSCCVMGD7
    WHERE 기준년월일 = '{friday_b01w}'
) t1
LEFT JOIN (
    SELECT
          기준년월일
        , 채널명 AS 채널
        , 고객경험단계명 AS 고객경험단계
        , NPS점수 AS NPS
    FROM inst1.TSCCVMGD7
    WHERE 기준년월일 = '{friday_b02w}'
) t2
   ON t1.채널 = t2.채널
  AND t1.고객경험단계 = t2.고객경험단계