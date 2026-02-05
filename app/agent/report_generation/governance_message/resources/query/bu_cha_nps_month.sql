SELECT 기준년월,
기준년월일,
채널명 AS 채널,
NPS점수 AS NPS,
전체건수,
추천건수,
추천비율,
중립건수,
중립비율,
비추천건수,
비추천비율
FROM inst1.TSCCVMGD6
WHERE 채널명 = REPLACE('{{channel_type}}', 'KB ', '') /* channel_type 채널 (KB 스타뱅킹이 스타뱅킹으로 나옴) */
;