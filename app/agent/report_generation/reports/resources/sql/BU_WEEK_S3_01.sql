SELECT VOC원문내용 AS VOC
FROM inst1.TSCCVMGF4
WHERE 1=1
  AND 기준년월일 BETWEEN '{monday_b01w}' AND '{biz_endday_b01w}'
  AND 채널명 = '{channel}'
  AND 고객경험VOC유형명 = '불만'
  AND LENGTH(REPLACE(VOC원문내용, ' ', '')) BETWEEN 10 AND 200