SELECT VOC원문내용 AS VOC
FROM inst1.TSCCVMGF4
WHERE 1=1
  AND SUBSTR(기준년월일, 1, 6) = '{yyyymm_b01m}'
  AND 채널명 = '{channel}'
  AND 고객경험VOC유형명 = '불만'
  AND LENGTH(REPLACE(VOC원문내용, ' ', '')) BETWEEN 10 AND 200
ORDER BY LENGTH(REPLACE(VOC원문내용, ' ', '')) DESC
LIMIT 300