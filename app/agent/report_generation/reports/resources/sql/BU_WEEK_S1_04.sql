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
        JOIN inst1.vsccvsv73 J ON A.그룹회사코드 = J.그룹회사코드 AND A.설문ID = J.설문ID AND A.설문참여대상자고유ID = J.설문참여대상자고유ID AND A.문항ID = J.문항ID AND J.문항구분 = '01' and J.질문의도대구분 = '09'
        WHERE A.그룹회사코드 = 'KB0'
          AND A.기준년월 >= '202503'
          AND A.관리설정여부 = 1
          AND A.응답완료년월일 >= '20250301'
          AND IF(A.응답완료년월일 >= '20250301', A.개선부서분배여부 = 1, 1=1)
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
), B AS (
    SELECT
        mg57.기준년월일 AS 응답완료년월일,
        mg57.설문참여대상자고유ID,
        COALESCE(배분.조사채널, 
            CASE 
                WHEN mg57.고객경험단계구분 IN ('07','08','09','10','11','12') THEN '상품'
                WHEN mg57.고객경험단계구분 NOT IN ('07','08','09','10','11','12') AND mg57.문항설문조사대상구분 = '06' THEN 'KB 스타뱅킹'
                WHEN mg57.고객경험단계구분 NOT IN ('07','08','09','10','11','12') AND mg57.문항설문조사대상구분 = '07' THEN '영업점'
                WHEN mg57.고객경험단계구분 NOT IN ('07','08','09','10','11','12') AND mg57.문항설문조사대상구분 = '08' THEN '고객센터'
            END) AS 조사채널,
        substr(mg57.고객경험단계코드,-3) as CX코드,
        COALESCE(배분.고객경험단계, ci04.인스턴스내용) AS 고객경험단계,
        CI07.서비스품질요소코드 as SQ코드,
        CI07.서비스품질요소명 AS 서비스품질요소,
        sv73.문항응답내용 AS 고객경험VOC내용,
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
        배분.검토여부,
        배분.부서검토의견,
        배분.과제추진사업내용
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
    LEFT JOIN inst1.vsccvsv73 sv73 ON mg57.그룹회사코드 = sv73.그룹회사코드
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
      AND sv73.그룹회사코드 = 'KB0'
      AND sv73.설문조사방식구분 = '02'
      AND sv73.설문조사종류구분 = '03'
    group by 응답완료년월일, 설문참여대상자고유ID, 고객경험VOC내용
), TMP1 AS (
    SELECT
          조사채널 AS 채널
        , CASE
            WHEN 조사채널='KB 스타뱅킹' AND 그룹내용 NOT IN ('디지털영업그룹', '개인고객그룹', 'WM고객그룹', '기업고객그룹') THEN '기타'
            WHEN 조사채널='영업점' AND 그룹내용 NOT IN ('추진그룹') THEN '기타'
            WHEN 조사채널='고객센터' AND 그룹내용 NOT IN ('고객컨택영업그룹') THEN '기타'
            WHEN 조사채널='상품' AND 그룹내용 NOT IN ('개인고객그룹', 'WM고객그룹', '기업고객그룹') THEN '기타'
            ELSE 그룹내용
          END AS 그룹명
        , COUNT(1) AS 인입
        , SUM(CASE WHEN 고객감정 = '긍정' THEN 1 ELSE 0 END) AS 긍정
        , SUM(CASE WHEN 고객감정 = '부정' THEN 1 ELSE 0 END) AS 부정
        , SUM(CASE WHEN 고객감정 = '중립' THEN 1 ELSE 0 END) AS 중립
        , SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END) AS 배분
        , SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) AS 검토
        , SUM(CASE WHEN 검토구분 = '현행유지' THEN 1 ELSE 0 END) AS 현행유지
        , SUM(CASE WHEN 검토구분 = '개선불가' THEN 1 ELSE 0 END) AS 개선불가
        , SUM(CASE WHEN 검토구분 = '개선예정' THEN 1 ELSE 0 END) AS 개선예정
        , ROUND(SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END), 1) AS 검토율
    FROM B
    WHERE 응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          조사채널 AS 채널
        , '소계' AS 그룹명
        , COUNT(1) AS 인입
        , SUM(CASE WHEN 고객감정 = '긍정' THEN 1 ELSE 0 END) AS 긍정
        , SUM(CASE WHEN 고객감정 = '부정' THEN 1 ELSE 0 END) AS 부정
        , SUM(CASE WHEN 고객감정 = '중립' THEN 1 ELSE 0 END) AS 중립
        , SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END) AS 배분
        , SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) AS 검토
        , SUM(CASE WHEN 검토구분 = '현행유지' THEN 1 ELSE 0 END) AS 현행유지
        , SUM(CASE WHEN 검토구분 = '개선불가' THEN 1 ELSE 0 END) AS 개선불가
        , SUM(CASE WHEN 검토구분 = '개선예정' THEN 1 ELSE 0 END) AS 개선예정
        , ROUND(SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END), 1) AS 검토율
    FROM B
    WHERE 응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '합계' AS 채널
        , '' AS 그룹명
        , COUNT(1) AS 인입
        , SUM(CASE WHEN 고객감정 = '긍정' THEN 1 ELSE 0 END) AS 긍정
        , SUM(CASE WHEN 고객감정 = '부정' THEN 1 ELSE 0 END) AS 부정
        , SUM(CASE WHEN 고객감정 = '중립' THEN 1 ELSE 0 END) AS 중립
        , SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END) AS 배분
        , SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) AS 검토
        , SUM(CASE WHEN 검토구분 = '현행유지' THEN 1 ELSE 0 END) AS 현행유지
        , SUM(CASE WHEN 검토구분 = '개선불가' THEN 1 ELSE 0 END) AS 개선불가
        , SUM(CASE WHEN 검토구분 = '개선예정' THEN 1 ELSE 0 END) AS 개선예정
        , ROUND(SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END), 1) AS 검토율
    FROM B
    WHERE 응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b01w}'
    GROUP BY 1, 2
), TMP2 AS (
    SELECT
          조사채널 AS 채널
        , CASE
            WHEN 조사채널='KB 스타뱅킹' AND 그룹내용 NOT IN ('디지털영업그룹', '개인고객그룹', 'WM고객그룹', '기업고객그룹') THEN '기타'
            WHEN 조사채널='영업점' AND 그룹내용 NOT IN ('추진그룹') THEN '기타'
            WHEN 조사채널='고객센터' AND 그룹내용 NOT IN ('고객컨택영업그룹') THEN '기타'
            WHEN 조사채널='상품' AND 그룹내용 NOT IN ('개인고객그룹', 'WM고객그룹', '기업고객그룹') THEN '기타'
            ELSE 그룹내용
          END AS 그룹명
        , COUNT(1) AS 인입
        , SUM(CASE WHEN 고객감정 = '긍정' THEN 1 ELSE 0 END) AS 긍정
        , SUM(CASE WHEN 고객감정 = '부정' THEN 1 ELSE 0 END) AS 부정
        , SUM(CASE WHEN 고객감정 = '중립' THEN 1 ELSE 0 END) AS 중립
        , SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END) AS 배분
        , SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) AS 검토
        , SUM(CASE WHEN 검토구분 = '현행유지' THEN 1 ELSE 0 END) AS 현행유지
        , SUM(CASE WHEN 검토구분 = '개선불가' THEN 1 ELSE 0 END) AS 개선불가
        , SUM(CASE WHEN 검토구분 = '개선예정' THEN 1 ELSE 0 END) AS 개선예정
        , ROUND(SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END), 1) AS 검토율
    FROM B
    WHERE 응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b02w}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          조사채널 AS 채널
        , '소계' AS 그룹명
        , COUNT(1) AS 인입
        , SUM(CASE WHEN 고객감정 = '긍정' THEN 1 ELSE 0 END) AS 긍정
        , SUM(CASE WHEN 고객감정 = '부정' THEN 1 ELSE 0 END) AS 부정
        , SUM(CASE WHEN 고객감정 = '중립' THEN 1 ELSE 0 END) AS 중립
        , SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END) AS 배분
        , SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) AS 검토
        , SUM(CASE WHEN 검토구분 = '현행유지' THEN 1 ELSE 0 END) AS 현행유지
        , SUM(CASE WHEN 검토구분 = '개선불가' THEN 1 ELSE 0 END) AS 개선불가
        , SUM(CASE WHEN 검토구분 = '개선예정' THEN 1 ELSE 0 END) AS 개선예정
        , ROUND(SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END), 1) AS 검토율
    FROM B
    WHERE 응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b02w}'
    GROUP BY 1, 2
    UNION ALL
    SELECT
          '합계' AS 채널
        , '' AS 그룹명
        , COUNT(1) AS 인입
        , SUM(CASE WHEN 고객감정 = '긍정' THEN 1 ELSE 0 END) AS 긍정
        , SUM(CASE WHEN 고객감정 = '부정' THEN 1 ELSE 0 END) AS 부정
        , SUM(CASE WHEN 고객감정 = '중립' THEN 1 ELSE 0 END) AS 중립
        , SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END) AS 배분
        , SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) AS 검토
        , SUM(CASE WHEN 검토구분 = '현행유지' THEN 1 ELSE 0 END) AS 현행유지
        , SUM(CASE WHEN 검토구분 = '개선불가' THEN 1 ELSE 0 END) AS 개선불가
        , SUM(CASE WHEN 검토구분 = '개선예정' THEN 1 ELSE 0 END) AS 개선예정
        , ROUND(SUM(CASE WHEN 검토여부 <> '미검토' THEN 1 ELSE 0 END) * 100.0 / SUM(CASE WHEN 배분여부 <> '미배분' THEN 1 ELSE 0 END), 1) AS 검토율
    FROM B
    WHERE 응답완료년월일 BETWEEN '{yyyymmdd_cx_manage}' AND '{biz_endday_b02w}'
    GROUP BY 1, 2
), ALL_COMBO AS (                                   -- ① 모든 채널‑그룹 조합을 미리 만든 테이블
    SELECT 'KB 스타뱅킹' AS 채널, '디지털영업그룹'   AS 그룹명 UNION ALL
    SELECT 'KB 스타뱅킹', '개인고객그룹'   UNION ALL
    SELECT 'KB 스타뱅킹', 'WM고객그룹'    UNION ALL
    SELECT 'KB 스타뱅킹', '기업고객그룹'  UNION ALL
    SELECT 'KB 스타뱅킹', '기타'          UNION ALL
    SELECT 'KB 스타뱅킹', '소계'          UNION ALL
    SELECT '영업점',      '영업그룹'       UNION ALL
    SELECT '영업점',      '기타'          UNION ALL
    SELECT '영업점',      '소계'          UNION ALL
    SELECT '고객센터',    '고객컨택영업그룹' UNION ALL
    SELECT '고객센터',    '기타'          UNION ALL
    SELECT '고객센터',    '소계'          UNION ALL
    SELECT '상품',        '개인고객그룹'   UNION ALL
    SELECT '상품',        'WM고객그룹'    UNION ALL
    SELECT '상품',        '기업고객그룹'  UNION ALL
    SELECT '상품',        '기타'          UNION ALL
    SELECT '상품',        '소계'          UNION ALL
    SELECT '합계',        ''               -- 합계 행
)
SELECT
      BO.채널
    , BO.그룹명
    , IFNULL(t1.인입, '-') AS 인입
    , IFNULL(t1.긍정, '-') AS 긍정
    , IFNULL(t1.부정, '-') AS 부정
    , IFNULL(t1.중립, '-') AS 중립
    , IFNULL(t1.배분, '-') AS 배분
    , IFNULL(t1.검토, '-') AS 검토
    , IFNULL(t1.현행유지, '-') AS 현행유지
    , IFNULL(t1.개선불가, '-') AS 개선불가
    , IFNULL(t1.개선예정, '-') AS 개선예정
    , IFNULL(t1.검토율, '-') AS 검토율
    , IFNULL(t1.검토율 - t2.검토율, '-') AS 검토율변동
from ALL_COMBO BO
LEFT JOIN TMP1 t1
ON BO.채널 = t1.채널
AND BO.그룹명 = t1.그룹명
LEFT JOIN TMP2 t2
ON t1.채널 = t2.채널
AND t1.그룹명 = t2.그룹명
ORDER BY
  CASE
    WHEN BO.채널 = 'KB 스타뱅킹' AND BO.그룹명 = '디지털영업그룹' THEN 1
    WHEN BO.채널 = 'KB 스타뱅킹' AND BO.그룹명 = '개인고객그룹' THEN 2
    WHEN BO.채널 = 'KB 스타뱅킹' AND BO.그룹명 = 'WM고객그룹' THEN 3
    WHEN BO.채널 = 'KB 스타뱅킹' AND BO.그룹명 = '기업고객그룹' THEN 4
    WHEN BO.채널 = 'KB 스타뱅킹' AND BO.그룹명 = '기타' THEN 5
    WHEN BO.채널 = 'KB 스타뱅킹' AND BO.그룹명 = '소계' THEN 6
    WHEN BO.채널 = '영업점'      AND BO.그룹명 = '영업그룹' THEN 7
    WHEN BO.채널 = '영업점'      AND BO.그룹명 = '기타' THEN 8
    WHEN BO.채널 = '영업점'      AND BO.그룹명 = '소계' THEN 9
    WHEN BO.채널 = '고객센터'    AND BO.그룹명 = '고객컨택영업그룹' THEN 10
    WHEN BO.채널 = '고객센터'    AND BO.그룹명 = '기타' THEN 11
    WHEN BO.채널 = '고객센터'    AND BO.그룹명 = '소계' THEN 12
    WHEN BO.채널 = '상품'        AND BO.그룹명 = '개인고객그룹' THEN 13
    WHEN BO.채널 = '상품'        AND BO.그룹명 = 'WM고객그룹' THEN 14
    WHEN BO.채널 = '상품'        AND BO.그룹명 = '기업고객그룹' THEN 15
    WHEN BO.채널 = '상품'        AND BO.그룹명 = '기타' THEN 16
    WHEN BO.채널 = '상품'        AND BO.그룹명 = '소계' THEN 17
    WHEN BO.채널 = '합계'        AND BO.그룹명 = '' THEN 18
  end