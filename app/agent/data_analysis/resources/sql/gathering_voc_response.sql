WITH
rcmdn_resn AS (
    SELECT
          resn.그룹회사코드
        , resn.기준년월일
        , resn.설문ID
        , resn.설문참여대상자고유ID
        , resn.문항ID
        , resn.설문조사방식구분
        , resn.설문조사종류구분
        , resn.설문조사대상구분
        , resn.고객경험단계구분
        , resn.추천이유문항ID
        , qusn_qsitm.문항제목명 AS 추천이유문항제목
        , qusn_qsitm.문항설명내용 AS 추천이유문항설명
        , resn.추천이유문항응답내용
        , resn.문항서비스품질요소코드
    FROM (
        SELECT
              sv73.그룹회사코드
            , sv73.설문응답종료년월일 AS 기준년월일
            , sv73.설문ID
            , sv73.설문참여대상자고유ID
            , sv73.설문조사방식구분
            , sv73.설문조사종류구분
            , sv73.설문조사대상구분
            , sv73.고객경험단계구분
            , MAX(CASE WHEN sv73.질문의도대구분 = '09' THEN sv73.문항ID END) AS 문항ID
            , MAX(CASE WHEN sv73.질문의도대구분 = '13' THEN sv73.문항ID END) AS 추천이유문항ID
            , MAX(CASE WHEN sv73.질문의도대구분 = '13' THEN sv73.문항응답내용 END) AS 추천이유문항응답내용
            , MAX(CASE WHEN sv73.질문의도대구분 = '13' THEN sv73.서비스품질요소코드 END) AS 문항서비스품질요소코드
        FROM inst1.vsccvsv73 sv73
        WHERE 1=1
          AND sv73.그룹회사코드 = 'KB0'
          AND sv73.설문응답종료년월일 BETWEEN '{start_ymd}' AND '{end_ymd}'
          AND sv73.사용여부 = '1'
          AND sv73.설문조사종류구분 IN ( '01', '03' ) -- NPS 조사(TD), 고객경험 만족도 조사(BU)
          AND sv73.고객경험단계구분 NOT IN ('03') -- 직원 (자가진단용) 제외
          AND sv73.문항응답내용 IS NOT NULL
          AND CHAR_LENGTH(sv73.문항응답내용) > 10 -- 10음절 밑으로 지우기
          AND (
              sv73.질문의도대구분 = '09' -- 구체적 경험
              OR (sv73.질문의도대구분 = '13' AND sv73.문항구분 != '01') -- `추천이유` 이면서 Not 서술형
          )
          AND sv73.사용여부 = '1'
        GROUP BY 1,2,3,4,5,6,7,8
    ) resn
    INNER JOIN inst1.TSCCVSV11 qusn_fxdfm -- 설문 양식
         ON qusn_fxdfm.그룹회사코드 = resn.그룹회사코드
        AND qusn_fxdfm.설문ID = resn.설문ID
        AND qusn_fxdfm.사용여부 = '1'
    INNER JOIN inst1.TSCCVSV04 qusn_qsitm -- 설문 문항 (추천이유)
         ON qusn_qsitm.그룹회사코드 = resn.그룹회사코드
        AND qusn_qsitm.설문양식ID = qusn_fxdfm.설문양식ID
        AND qusn_qsitm.문항ID = resn.추천이유문항ID
        AND qusn_qsitm.사용여부 = '1'
)


SELECT 
      vocRes.그룹회사코드
    , vocRes.기준년월일
    , vocRes.설문ID
    , vocRes.설문참여대상자고유ID
    , vocRes.문항ID
    , target.거래은행구분
    , bank.인스턴스내용 AS 거래은행구분명
    , vocRes.설문조사방식구분
    , polMod.인스턴스내용 AS 설문조사방식구분명
    , vocRes.설문조사종류구분
    , vocRes.문항설문조사대상구분
    , polTrg.인스턴스내용 AS 문항설문조사대상구분명
    , vocRes.질문의도대구분
    , qryIntenLag.인스턴스내용 AS 질문의도대구분명
    , vocRes.고객경험단계구분
    , cxStgeDstic.인스턴스내용 AS 고객경험단계구분명
    , vocRes.문항응답내용
    , rcmdn_resn.추천이유문항ID
    , rcmdn_resn.추천이유문항제목
    , rcmdn_resn.추천이유문항설명
    , rcmdn_resn.추천이유문항응답내용
    , rcmdn_resn.문항서비스품질요소코드
FROM (
    SELECT 
          *
        , ROW_NUMBER() OVER (
            ORDER BY voc.그룹회사코드,
                     voc.기준년월일,
                     voc.설문ID,
                     voc.설문참여대상자고유ID,
                     voc.문항ID
        ) AS RN
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
    ) voc
    WHERE 1=1
      AND voc.설문조사방식구분 {pol_mod_param}
      AND voc.문항설문조사대상구분 {chnl_param}
) vocRes
    INNER JOIN inst1.TSCCVSV22 target
         ON target.그룹회사코드 = vocRes.그룹회사코드
        AND target.설문참여대상자고유ID = vocRes.설문참여대상자고유ID
        AND target.사용여부 = '1'
    LEFT JOIN inst1.TSCCVCI04 bank
         ON bank.그룹회사코드 = target.그룹회사코드
        AND bank.인스턴스식별자 = '142482000'
        AND bank.인스턴스코드 = CONCAT("0", target.거래은행구분)
    LEFT JOIN inst1.TSCCVCI04 polMod
         ON polMod.그룹회사코드 = vocRes.그룹회사코드
        AND polMod.인스턴스식별자 = '142448000'
        AND polMod.인스턴스코드 = vocRes.설문조사방식구분
    LEFT JOIN inst1.TSCCVCI04 qryIntenLag
         ON qryIntenLag.그룹회사코드 = vocRes.그룹회사코드
        AND qryIntenLag.인스턴스식별자 = '142457000'
        AND qryIntenLag.인스턴스코드 = vocRes.질문의도대구분
    LEFT JOIN inst1.TSCCVCI04 polTrg
         ON polTrg.그룹회사코드 = vocRes.그룹회사코드
        AND polTrg.인스턴스식별자 = '142447000'
        AND polTrg.인스턴스코드 = vocRes.문항설문조사대상구분
    LEFT JOIN inst1.TSCCVCI04 cxStgeDstic
         ON cxStgeDstic.그룹회사코드 = vocRes.그룹회사코드
        AND cxStgeDstic.인스턴스식별자 = '142594000'
        AND cxStgeDstic.인스턴스코드 = vocRes.고객경험단계구분
    LEFT JOIN rcmdn_resn
         ON rcmdn_resn.그룹회사코드 = vocRes.그룹회사코드
        AND rcmdn_resn.기준년월일 = vocRes.기준년월일
        AND rcmdn_resn.설문ID = vocRes.설문ID
        AND rcmdn_resn.설문참여대상자고유ID = vocRes.설문참여대상자고유ID
        AND rcmdn_resn.문항ID = vocRes.문항ID
        AND vocRes.질문의도대구분 = '09' -- `구체적 경험`인 VOC
WHERE 1=1
  AND vocRes.RN BETWEEN {rn_start} AND {rn_end}