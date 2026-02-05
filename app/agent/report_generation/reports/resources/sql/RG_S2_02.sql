WITH NOW AS (
    SELECT ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '불만' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 금주CCI
    FROM inst1.TSCCVMGF4
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
), LAST_MONTH AS (
    SELECT ROUND(SUM(CASE WHEN 고객경험VOC유형명 = '불만' THEN 1 ELSE 0 END) * 100.0 / COUNT(1), 1) AS 전월CCI
    FROM inst1.TSCCVMGF4
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{yyyymm_b01m}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
)
SELECT 
      (SELECT 금주CCI FROM NOW) AS 금주CCI
    , (SELECT 전월CCI FROM LAST_MONTH) AS 전월CCI
    , ROUND((SELECT 금주CCI FROM NOW) - (SELECT 전월CCI FROM LAST_MONTH), 1) AS CCI변동
    , CASE
        WHEN ROUND((SELECT 금주CCI FROM NOW) - (SELECT 전월CCI FROM LAST_MONTH), 1) > 0 THEN '증가'
        WHEN ROUND((SELECT 금주CCI FROM NOW) - (SELECT 전월CCI FROM LAST_MONTH), 1) < 0 THEN '감소'
        ELSE '-'
      END AS CCI변동구분