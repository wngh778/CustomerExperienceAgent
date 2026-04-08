SELECT C.CHNL, C.CXE_CD, C.WORD, C.PRODUCT_SERVICE_YN
FROM (
    -- 상품서비스용어
    SELECT A.문항설문조사대상구분 AS CHNL, A.고객경험요소구분 AS CXE_CD, A.상품서비스용어내용 AS WORD, '1' AS PRODUCT_SERVICE_YN 
    FROM (
        SELECT 문항설문조사대상구분, 고객경험요소구분, 상품서비스용어내용, ROW_NUMBER() OVER (PARTITION BY 문항설문조사대상구분, 고객경험요소구분 ORDER BY 설문응답종료년월일) AS RN
        FROM inst1.TSCCVSV50
        WHERE 상품서비스용어내용 IS NOT NULL and 상품서비스용어내용 != ''
    ) A
    WHERE A.RN <= {prev_word_size}

    UNION ALL

    -- 성능품질용어
    SELECT B.문항설문조사대상구분 AS CHNL, B.고객경험요소구분 AS CXE_CD, B.성능품질용어내용 AS WORD, '0' AS PRODUCT_SERVICE_YN 
    FROM (
        SELECT 문항설문조사대상구분, 고객경험요소구분, 성능품질용어내용, ROW_NUMBER() OVER (PARTITION BY 문항설문조사대상구분, 고객경험요소구분 ORDER BY 설문응답종료년월일) AS RN
        FROM inst1.TSCCVSV50
        WHERE 성능품질용어내용 IS NOT NULL and 성능품질용어내용 != ''
    ) B
    WHERE B.RN <= {prev_word_size}
) C;