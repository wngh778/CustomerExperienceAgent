SELECT
      고객경험단계
    , VOC원문내용 AS VOC원문
FROM (
    SELECT
          고객경험단계명 AS 고객경험단계
        , VOC원문내용
        , ROW_NUMBER() OVER(
            PARTITION BY 채널명, 고객경험단계명
            ORDER BY LENGTH(REPLACE(VOC원문내용, ' ', '')) DESC
        ) AS rn
    FROM inst1.TSCCVMGD3
    WHERE 1=1
      AND 조사년도 = '{yyyy}'
      AND 반기구분명 = '{yyyyhf}'
      AND 채널명 = '{channel}'
) t
WHERE rn <= 50