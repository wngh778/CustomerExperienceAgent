SELECT 
    COUNT(1) AS total_elements
FROM (
    SELECT
          sv73.그룹회사코드
        , sv73.설문응답종료년월일 AS 기준년월일
        , sv73.설문ID
        , sv73.설문참여대상자고유ID
        , sv73.문항ID
        , sv73.설문조사방식구분
        , sv73.설문조사종류구분
        , sv73.질문의도대구분
        , sv73.설문조사대상구분
        , CASE
            WHEN sv73.문항설문조사대상구분 IN ('06', '07')
            THEN (CASE
                    WHEN sv73.고객경험단계구분 IN ('07', '08', '09', '10', '11', '12')
                    THEN '09'
                    ELSE sv73.문항설문조사대상구분
                 END)
            ELSE sv73.문항설문조사대상구분
          END AS 문항설문조사대상구분  -- BU, 영업점, 스타뱅킹일 때, 특정 단계는 채널은 상품('09')
        , sv73.고객경험단계구분
        , sv73.문항응답내용
    FROM inst1.vsccvsv73 sv73
    WHERE 1=1
      AND sv73.그룹회사코드 = 'KB0'
      AND sv73.설문응답종료년월일 BETWEEN '{start_ymd}' AND '{end_ymd}'
      AND sv73.사용여부 = '1'
      AND sv73.설문조사종류구분 IN ( '01', '03' ) -- NPS 조사(TD), 고객경험 만족도 조사(BU)
      AND sv73.문항구분 IN ('01') -- 서술형
      AND sv73.문항설문조사대상구분 != '00'
      AND sv73.고객경험단계구분 NOT IN ('03') -- 직원 (자가진단용) 제외
      AND sv73.문항응답내용 IS NOT NULL
      AND CHAR_LENGTH(sv73.문항응답내용) > 10 -- 10음절 밑으로 지우기
) vocRes
INNER JOIN inst1.TSCCVSV22 target
     ON target.그룹회사코드 = vocRes.그룹회사코드
    AND target.설문참여대상자고유ID = vocRes.설문참여대상자고유ID
    AND target.사용여부 = '1'
WHERE 1=1
  AND vocRes.설문조사방식구분 {pol_mod_param}
  AND vocRes.문항설문조사대상구분 {chnl_param}