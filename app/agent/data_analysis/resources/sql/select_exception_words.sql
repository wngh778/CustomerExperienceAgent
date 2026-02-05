SELECT
      standard_word.고객접점용어ID
    , standard_word.고객접점용어명
    , standard_word.표준용어유형구분
    , similar_word.고객접점유사단어명
FROM inst1.TSCCVMG95 standard_word
    INNER JOIN inst1.TSCCVMG85 similar_word
         ON similar_word.그룹회사코드 = standard_word.그룹회사코드
        AND similar_word.고객접점용어ID = standard_word.고객접점용어ID
        AND similar_word.사용여부 = '1'
WHERE standard_word.그룹회사코드 = 'KB0'
  AND standard_word.표준용어유형구분 IN ('03') -- 불용어
  AND standard_word.사용여부 = '1'