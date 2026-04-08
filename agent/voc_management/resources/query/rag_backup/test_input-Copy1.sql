SELECT *
FROM (
  SELECT
      기준년월일,
      문항설문조사대상구분,
      조사채널내용,
      고객경험단계구분,
      고객경험단계내용,
      서비스품질요소코드,
      문항응답내용,
      과제검토의견내용,
      ROW_NUMBER() OVER (
        PARTITION BY 문항설문조사대상구분, 고객경험단계구분, 서비스품질요소코드
        ORDER BY 기준년월일 DESC  -- 또는 원하는 기준
      ) AS rn
  FROM INST1.TSCCVMGB3
  WHERE 기준년월일 BETWEEN '20240101' AND '20251231'
    AND 고객감정대분류구분 = '02'
    AND 고객경험VOC유형구분 IN ('02','03')
) t
WHERE rn <= 5   -- 그룹당 최대 5건
ORDER BY rn,
         문항설문조사대상구분,
         고객경험단계구분,
         서비스품질요소코드
LIMIT 1;


