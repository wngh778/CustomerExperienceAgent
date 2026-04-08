WITH NOW AS (
    SELECT
            ROUND((SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END)
          - SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END)) * 100.0 / COUNT(1), 1)
          AS 금주NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{friday_b01w}'
      AND 지역영업그룹명 = '{region_group_name}'
      AND 채널명 = '영업점' AND 고객경험단계명 = '해당무'
), LAST_MONTH AS (
    SELECT
            ROUND((SUM(CASE WHEN 추천점수 IN (9, 10)               THEN 1 ELSE 0 END)
          - SUM(CASE WHEN 추천점수 IN (0, 1, 2, 3, 4, 5, 6) THEN 1 ELSE 0 END)) * 100.0 / COUNT(1), 1)
          AS 전월NPS
    FROM inst1.TSCCVMGF3
    WHERE 1=1
      AND 기준년월일 BETWEEN '{yyyy0101}' AND '{yyyymm_b01m}'
      AND 지역영업그룹명 = '{region_group_name}'
      AND 채널명 = '영업점' AND 고객경험단계명 = '해당무'
)
SELECT 
      (SELECT 금주NPS FROM NOW) AS 금주NPS
    , (SELECT 전월NPS FROM LAST_MONTH) AS 전월NPS
    , ROUND((SELECT 금주NPS FROM NOW) - (SELECT 전월NPS FROM LAST_MONTH), 1) AS NPS변동
    , CASE
        WHEN ROUND((SELECT 금주NPS FROM NOW) - (SELECT 전월NPS FROM LAST_MONTH), 1) > 0 THEN '증가'
        WHEN ROUND((SELECT 금주NPS FROM NOW) - (SELECT 전월NPS FROM LAST_MONTH), 1) < 0 THEN '감소'
        ELSE '-'
      END AS NPS변동구분