WITH 배분 AS (
    WITH A AS (
        SELECT
            A.그룹회사코드,
            A.고객식별자,
            A.설문ID,
            A.문항ID,
            A.설문참여대상자고유ID,
            A.응답완료년월일,
            LEFT(B.시스템최종처리일시, 8) AS 처리년월일,
            A.기준년월,
            A.설문조사대상구분,
            SUBSTR(A.고객접점중분류코드, 4, 3) AS CX코드,
            A.고객경험단계구분,
            A.서비스품질요소코드,
            A.부점번호,
            A.회계부점코드,
            A.고객감정대분류구분,
            A.고객경험VOC유형구분,
            IFNULL(B.개선조치검토ID, '') AS 개선조치검토ID,
            C.과제진행상태구분,
            CASE
                WHEN D.과제검토구분 = '01' THEN '현행유지'
                WHEN D.과제검토구분 = '02' THEN '개선예정'
                WHEN D.과제검토구분 = '03' THEN '개선불가'
                ELSE ''
            END AS 검토구분,
            D.개선이행종료년월일,
            D.피드백발송여부,
            (SELECT 익7영업년월일 FROM inst1.tsccvci12 WHERE 기준년월일 = A.응답완료년월일) AS flag,
            CASE
                WHEN D.피드백발송여부 = '1' THEN '1'
                ELSE '0'
            END AS fcbFlag,
            J.문항응답내용,
            D.과제검토의견내용,
            D.과제추진사업내용
        FROM inst1.tsccvmg27 A
        LEFT JOIN inst1.tsccvmg84 B ON A.그룹회사코드 = B.그룹회사코드 AND A.설문ID = B.설문ID AND A.문항ID = B.문항ID AND A.설문참여대상자고유ID = B.설문참여대상자고유ID AND A.순서일련번호 = B.순서일련번호 AND A.응답완료년월일 = B.응답완료년월일
        LEFT JOIN inst1.tsccvmg81 C ON B.그룹회사코드 = C.그룹회사코드 AND B.설문ID = C.설문ID AND B.개선조치검토ID = C.개선조치검토ID
        LEFT JOIN inst1.tsccvmg82 D ON C.그룹회사코드 = D.그룹회사코드 AND C.설문ID = D.설문ID AND C.개선조치검토ID = D.개선조치검토ID AND D.최종데이터여부 = '1'
        LEFT JOIN inst1.tsccvmga4 E ON A.그룹회사코드 = E.그룹회사코드 AND A.서비스품질요소코드 = E.서비스품질요소코드 AND E.사용여부 = '1'
        JOIN inst1.tsccvsv73 J ON A.그룹회사코드 = J.그룹회사코드 AND A.설문ID = J.설문ID AND A.설문참여대상자고유ID = J.설문참여대상자고유ID AND A.문항ID = J.문항ID AND J.문항구분 = '01' and J.질문의도대구분 = '09'
        WHERE A.그룹회사코드 = 'KB0'
          AND A.기준년월 >= '202503'
          AND A.관리설정여부 = 1
          AND A.응답완료년월일 >= '{yyyymmdd_cx_manage}'
          AND IF(A.응답완료년월일 >= '{yyyymmdd_cx_manage}', A.개선부서분배여부 = 1, 1=1)
          AND A.질문의도대구분 = '09'
          AND IF(A.설문조사대상구분 != '09', A.고객경험단계구분 NOT IN ('07', '08', '09', '10', '11', '12'), A.고객경험단계구분 IN ('07', '08', '09', '10', '11', '12'))
          AND IF(A.설문조사대상구분 IN ('06', '07', '08'), A.순서일련번호 = '1', 1=1)
          AND A.설문조사대상구분 IS NOT NULL
          AND A.고객경험단계구분 IS NOT NULL
          AND A.서비스품질요소코드 IS NOT NULL
    ),
    MAXD AS (
        SELECT DISTINCT
            A.그룹회사코드,
            A.설문ID,
            A.설문참여대상자고유ID,
            A.설문조사대상구분,
            A.CX코드,
            A.고객경험단계구분,
            A.서비스품질요소코드,
            A.검토구분,
            A.부점번호,
            A.회계부점코드,
            A.개선조치검토ID,
            A.응답완료년월일,
            A.처리년월일,
            A.과제진행상태구분,
            A.개선이행종료년월일,
            A.피드백발송여부,
            A.flag,
            A.fcbFlag,
            A.고객감정대분류구분,
            A.고객경험VOC유형구분,
            A.문항응답내용,
            A.과제검토의견내용,
            A.과제추진사업내용
        FROM A
        WHERE A.응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
    )
    SELECT
        B.응답완료년월일 AS 응답완료년월일,
        B.설문참여대상자고유ID,
        J.인스턴스내용 AS 조사채널,
        H.인스턴스내용 AS 고객경험단계,
        B.CX코드,
        B.고객경험단계구분 as 고객경험단계구분,
        B.서비스품질요소코드 AS SQ코드,
        K.서비스품질요소명 AS 서비스품질요소,
        B.문항응답내용 AS 고객경험_VOC_내용,
        CASE
            WHEN B.고객감정대분류구분 = '01' THEN '긍정'
            WHEN B.고객감정대분류구분 = '02' THEN '부정'
            WHEN B.고객감정대분류구분 = '03' THEN '중립'
        END AS 고객감정,
        B.고객감정대분류구분 as 고객감정대분류구분코드,
        case
            WHEN B.고객경험VOC유형구분 = '01' THEN '칭찬'
            WHEN B.고객경험VOC유형구분 = '02' THEN '불만'
            WHEN B.고객경험VOC유형구분 = '03' THEN '개선'
            WHEN B.고객경험VOC유형구분 = '99' THEN '기타'
        END AS voc유형구분,
        고객경험VOC유형구분 as voc유형코드,
        L.그룹내용 AS 그룹내용,
        L.부서명내용 AS 개선부서,
        '배분' AS 배분여부,
        B.검토구분 AS 검토구분,
        CASE
            WHEN B.과제진행상태구분 in('03','05') THEN '검토'
            ELSE '미검토'
        END AS 검토여부,
        B.처리년월일 AS 검토년월일,
        B.과제검토의견내용 AS 부서검토의견,
        B.과제추진사업내용 AS 과제추진사업내용
    FROM MAXD B
    LEFT JOIN inst1.tsccvci04 H ON B.그룹회사코드 = H.그룹회사코드 AND B.고객경험단계구분 = H.인스턴스코드 AND H.인스턴스식별자 = '142594000'
    LEFT JOIN inst1.tsccvci04 J ON B.그룹회사코드 = J.그룹회사코드 AND B.설문조사대상구분 = J.인스턴스코드 AND J.인스턴스식별자 = '142447000'
    LEFT JOIN (SELECT DISTINCT 그룹회사코드, 서비스품질요소코드, 서비스품질요소명 FROM inst1.tsccvci07) K ON B.그룹회사코드 = K.그룹회사코드 AND B.서비스품질요소코드 = K.서비스품질요소코드
    LEFT JOIN inst1.tsccvci11 L ON B.그룹회사코드 = L.그룹회사코드 AND B.부점번호 = L.부점번호
    group by 응답완료년월일, 설문참여대상자고유ID, 조사채널, 고객경험단계, 서비스품질요소, 고객경험_VOC_내용
    order by 응답완료년월일, 설문참여대상자고유ID, 조사채널, 고객경험단계, 서비스품질요소
)
SELECT
    mg57.기준년월일 AS 응답완료년월일,
    mg57.설문참여대상자고유ID,
    sv73.문항응답내용 AS 고객경험VOC내용,
    COALESCE(배분.조사채널, 
        CASE 
            WHEN mg57.고객경험단계구분 IN ('07','08','09','10','11','12') THEN '상품'
            WHEN mg57.고객경험단계구분 NOT IN ('07','08','09','10','11','12') AND mg57.문항설문조사대상구분 = '06' THEN 'KB 스타뱅킹'
            WHEN mg57.고객경험단계구분 NOT IN ('07','08','09','10','11','12') AND mg57.문항설문조사대상구분 = '07' THEN '영업점'
            WHEN mg57.고객경험단계구분 NOT IN ('07','08','09','10','11','12') AND mg57.문항설문조사대상구분 = '08' THEN '고객센터'
        END) AS 조사채널,
    COALESCE(배분.고객경험단계, ci04.인스턴스내용) AS 고객경험단계,
    CI07.서비스품질요소명 AS 서비스품질요소,
    CASE
        WHEN mg57.고객감정대분류구분 = '01' THEN '긍정'
        WHEN mg57.고객감정대분류구분 = '02' THEN '부정'
        WHEN mg57.고객감정대분류구분 = '03' THEN '중립'
    END AS 고객감정,
    case
        WHEN 배분.VOC유형구분 IS NOT NULL THEN 배분.VOC유형구분
        WHEN mg57.고객경험VOC유형구분 = '01' THEN '칭찬'
        WHEN mg57.고객경험VOC유형구분 = '02' THEN '불만'
        WHEN mg57.고객경험VOC유형구분 = '03' THEN '개선'
        WHEN mg57.고객경험VOC유형구분 = '99' THEN '기타'
    END AS voc유형구분,
    COALESCE(배분.그룹내용, ci11.그룹내용) AS 그룹내용,
    COALESCE(배분.개선부서, ci02.부점한글명) AS 개선부서,
    COALESCE(배분.배분여부, '미배분') AS 배분여부,
    배분.검토구분,
    배분.검토년월일,
    배분.검토여부
FROM inst1.tsccvmg57 mg57
LEFT JOIN 배분 ON mg57.기준년월일 = 배분.응답완료년월일
    AND mg57.설문참여대상자고유ID = 배분.설문참여대상자고유ID
    AND mg57.서비스품질요소코드 = 배분.SQ코드
    and mg57.고객경험VOC유형구분 = 배분.voc유형코드
    and mg57.고객경험단계구분 = 배분.고객경험단계구분
    and mg57.고객감정대분류구분 = 배분.고객감정대분류구분코드
LEFT JOIN inst1.tsccvci04 ci04 ON mg57.고객경험단계구분 = ci04.인스턴스코드
LEFT JOIN inst1.tsccvci07 ci07 ON mg57.서비스품질요소코드 = ci07.서비스품질요소코드
    AND mg57.문항설문조사대상구분 = ci07.설문조사대상구분
LEFT JOIN inst1.tsccvci11 ci11 ON mg57.그룹회사코드 = ci11.그룹회사코드
    AND mg57.부점번호 = ci11.부점번호
LEFT JOIN inst1.tsccvci02 ci02 ON mg57.그룹회사코드 = ci02.그룹회사코드
    AND mg57.회계부점코드 = ci02.부점코드
LEFT JOIN inst1.tsccvsv73 sv73 ON mg57.그룹회사코드 = sv73.그룹회사코드
    AND mg57.설문ID = sv73.설문ID
    AND mg57.설문참여대상자고유ID = sv73.설문참여대상자고유ID
    AND mg57.문항ID = sv73.문항ID
    and mg57.질문의도대구분 = sv73.질문의도대구분
    and mg57.문항설문조사대상구분 = sv73.설문조사대상구분
    AND mg57.고객경험단계구분 = sv73.고객경험단계구분
    AND sv73.문항구분 = '01'
WHERE mg57.기준년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
  AND mg57.설문조사방식구분 = '02'
  AND mg57.설문조사종류구분 = '03'
  AND mg57.그룹회사코드 = 'KB0'
  AND mg57.문항구분 = '01'
  AND mg57.질문의도대구분 = '09'
  AND ci04.인스턴스식별자 = '142594000'
  AND ci04.그룹회사코드 = 'KB0'
  AND ci07.설문조사종류구분 = '03'
  AND ci07.그룹회사코드 = 'KB0'
  AND ci07.설문조사방식구분 = '02'
  AND ci11.그룹내용 IS NOT NULL
  AND ci11.그룹회사코드 = 'KB0'
  AND sv73.설문응답종료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
  AND sv73.그룹회사코드 = 'KB0'
  AND sv73.설문조사방식구분 = '02'
  AND sv73.설문조사종류구분 = '03'
  -- and 배분여부 = '배분'
group by 응답완료년월일, 설문참여대상자고유ID, 고객경험VOC내용
ORDER by 응답완료년월일, 설문참여대상자고유ID, 조사채널, 고객경험단계, 서비스품질요소