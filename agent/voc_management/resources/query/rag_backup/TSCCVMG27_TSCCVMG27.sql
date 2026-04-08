SELECT
  g27.응답완료년월일,
  g27.고객식별자,
  g27.실질고객구분,
  g27.채널NPS점수,
  g27.NPS추천그룹구분,
  g27.고객경험관리등급,
  g27.고객감정대분류구분,
  g27.고객경험VOC유형구분,
  g27.설문조사대상구분,
  g27.고객경험단계구분,
  gb3.문항응답내용,
  gb3.인스턴스내용,
  gb3.개선부점코드,
  gb3.서비스품질요소코드
FROM inst1.TSCCVMG27 AS g27
INNER JOIN inst1.TSCCVMGB3 AS gb3
  ON g27.문항ID = gb3.문항ID
WHERE g27.고객감정대분류구분 IN ('02', '03')
LIMIT 10;