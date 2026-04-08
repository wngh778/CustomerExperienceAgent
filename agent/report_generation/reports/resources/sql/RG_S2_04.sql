WITH TMP AS (
    SELECT
          고객경험단계명
        , CASE
            WHEN 고객경험단계명 = '맞이/의도파악' THEN 1
            WHEN 고객경험단계명 = '직원상담' THEN 2
            WHEN 고객경험단계명 = '업무처리/배웅' THEN 3
            WHEN 고객경험단계명 = '내점/방문' THEN 4
            WHEN 고객경험단계명 = '대기' THEN 5
            ELSE 99
          END AS CX_pr
        , VOC문제원인내용 AS VOC
    FROM inst1.TSCCVMGF4
    WHERE 1=1
      AND 기준년월일 BETWEEN '{monday_b01w}' AND '{friday_b01w}'
      AND REPLACE(지역영업그룹명, '·', '') = '{region_group_name}'
      AND 채널명 = '영업점'
      AND 고객경험단계명 <> '해당무'
      AND 고객경험VOC유형명 = '불만'
      AND LENGTH(REPLACE(VOC문제원인내용, ' ', '')) BETWEEN 10 AND 200
)
SELECT
      고객경험단계명 AS 고객경험단계
    , VOC
FROM TMP t1
JOIN (
    SELECT DISTINCT CX_pr
    FROM TMP
    ORDER BY CX_pr
    LIMIT 3
) t2
ON t1.CX_pr = t2.CX_pr
ORDER BY
    CASE
      WHEN 고객경험단계명 = '맞이/의도파악' THEN 1
      WHEN 고객경험단계명 = '직원상담' THEN 2
      WHEN 고객경험단계명 = '업무처리/배웅' THEN 3
      WHEN 고객경험단계명 = '내점/방문' THEN 4
      WHEN 고객경험단계명 = '대기' THEN 5
      ELSE 99
    END