SELECT
    A.조사년도,
    A.반기구분명 AS 반기구분
FROM inst1.TSCCVMGC1 A
ORDER BY
    A.조사년도 DESC,                                 -- 연도 내림차순
    CASE
        WHEN A.반기구분명 = '하반기' THEN 0            -- 하반기 → 가장 앞
        WHEN A.반기구분명 = '상반기' THEN 1            -- 상반기 → 그 다음
        ELSE 2                                       -- 그 외 값은 뒤로
    END;