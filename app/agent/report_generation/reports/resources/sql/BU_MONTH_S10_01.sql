WITH A AS (
    SELECT
          A.그룹회사코드
        , A.설문ID
        , A.문항ID
        , A.설문참여대상자고유ID
        , A.기준년월
        , A.응답완료년월일
        , A.에피소드유형구분
        , G.인스턴스내용 AS 채널
        , H.인스턴스내용 AS 고객경험단계
        , I.서비스품질요소명 AS 서비스품질요소
        , A.부점번호
        , IFNULL(B.개선조치검토ID, '') AS 개선조치검토ID
        , C.과제진행상태구분
        , D.과제검토구분
        , D.개선이행종료년월일
        , D.피드백발송여부
        , ROW_NUMBER() OVER(PARTITION BY A.그룹회사코드, A.설문ID, A.문항ID, A.설문참여대상자고유ID, A.기준년월, A.응답완료년월일, A.에피소드유형구분, A.설문조사대상구분, A.고객경험단계구분, A.서비스품질요소코드) AS no
    FROM INST1.TSCCVMG27 A
    LEFT JOIN INST1.TSCCVMG84 B ON A.그룹회사코드=B.그룹회사코드 AND A.설문ID=B.설문ID AND A.문항ID=B.문항ID AND A.설문참여대상자고유ID=B.설문참여대상자고유ID AND A.순서일련번호=B.순서일련번호 AND A.응답완료년월일=B.응답완료년월일
    LEFT JOIN INST1.TSCCVMG81 C ON B.그룹회사코드=C.그룹회사코드 AND B.설문ID=C.설문ID AND B.개선조치검토ID=C.개선조치검토ID
    LEFT JOIN INST1.TSCCVMG82 D ON C.그룹회사코드=D.그룹회사코드 AND C.설문ID=D.설문ID AND C.개선조치검토ID=D.개선조치검토ID AND D.최종데이터여부='1'
    LEFT JOIN INST1.TSCCVMGA4 E ON A.그룹회사코드=E.그룹회사코드 AND A.서비스품질요소코드=E.서비스품질요소코드 AND A.에피소드유형구분=E.에피소드유형구분 AND E.사용여부='1' AND E.일련번호=1
    LEFT JOIN INST1.TSCCVMGA4 F ON A.그룹회사코드=F.그룹회사코드 AND A.서비스품질요소코드=F.서비스품질요소코드 AND F.에피소드유형구분='00' AND F.사용여부='1' AND F.일련번호=1
    LEFT JOIN INST1.TSCCVCI04 G ON A.그룹회사코드=G.그룹회사코드 AND A.설문조사대상구분=G.인스턴스코드 AND G.인스턴스식별자='142447000'
    LEFT JOIN INST1.TSCCVCI04 H ON A.그룹회사코드=H.그룹회사코드 AND A.고객경험단계구분=H.인스턴스코드 AND H.인스턴스식별자='142594000'
    LEFT JOIN INST1.TSCCVCI07 I ON A.그룹회사코드=I.그룹회사코드 AND A.서비스품질요소코드=I.서비스품질요소코드
    LEFT JOIN INST1.TSCCVCI11 J ON A.그룹회사코드=J.그룹회사코드 AND A.부점번호=J.부점번호
    WHERE A.그룹회사코드='KB0'
      AND A.관리설정여부=1
      AND A.응답완료년월일 >= '20250101'
      AND IF(A.응답완료년월일 >= '20250301', A.개선부서분배여부='1', 1=1)
      AND A.질문의도대구분='09'
      AND IF(A.설문조사대상구분 != '09', A.고객경험단계구분 NOT IN ('07','08','09','10','11','12'), A.고객경험단계구분 IN ('07','08','09','10','11','12'))
      AND IF(A.설문조사대상구분 IN ('06','07','08'), A.순서일련번호='1', 1=1)
      AND A.설문조사대상구분 IS NOT NULL
      AND A.고객경험단계구분 IS NOT NULL
      AND A.서비스품질요소코드 IS NOT NULL
    GROUP BY A.그룹회사코드, A.설문ID, A.문항ID, A.설문참여대상자고유ID, A.기준년월, A.응답완료년월일, A.에피소드유형구분, A.설문조사대상구분, A.고객경험단계구분, A.서비스품질요소코드
), B AS (
    SELECT
          기준년월
        , 응답완료년월일
        , 채널
        , 고객경험단계
        , 서비스품질요소
        , CASE
            WHEN A.부점번호='100150' THEN '마이데이터부(P)'
            WHEN A.부점번호='102610' THEN '방카Unit'
            WHEN A.부점번호='105860' THEN '소비자보호부'
            WHEN A.부점번호='105951' THEN 'WM플랫폼부(P)'
            WHEN A.부점번호='110260' THEN '디지털영업부'
            WHEN A.부점번호='110280' THEN '수신상품부(P)'
            WHEN A.부점번호='110440' THEN '고객컨택혁신부(P)'
            WHEN A.부점번호='121060' THEN '총무부'
            WHEN A.부점번호='121680' THEN '채널혁신부'
            WHEN A.부점번호='121690' THEN '채널운영Unit(P)'
            WHEN A.부점번호='124020' THEN '개인여신부(P)'
            WHEN A.부점번호='127710' THEN '외환사업부'
            WHEN A.부점번호='129350' THEN 'WM투자상품부'
            WHEN A.부점번호='130630' THEN '고객컨택추진부'
            WHEN A.부점번호='132150' THEN '영업추진부'
            WHEN A.부점번호='135460' THEN '신탁부'
            WHEN A.부점번호='141500' THEN '외환업무부'
            WHEN A.부점번호='143070' THEN '주택기금Unit'
            WHEN A.부점번호='190450' THEN '인재개발부'
            WHEN A.부점번호='460002' THEN '스타뱅킹영업부(P)'
            ELSE B.부서명내용
          END AS 부서명
        , CASE
            WHEN 채널='영업점' AND B.그룹내용 NOT LIKE '%추진그룹' THEN '기타'
            ELSE B.그룹내용
          END AS 그룹명
        , SUM(CASE WHEN no='1' THEN 1 ELSE 0 END) AS 배분
        , 과제진행상태구분
        , CASE WHEN (과제진행상태구분='05' OR (과제진행상태구분='03' AND 피드백발송여부=0)) THEN SUM(CASE WHEN no='1' THEN 1 ELSE 0 END) ELSE 0 END AS 검토완료
        , CASE WHEN 과제진행상태구분='05' THEN COUNT(1) ELSE 0 END AS 피드백
        , CASE WHEN (개선조치검토ID IS NULL OR 과제진행상태구분 IN ('01','02','04')) THEN SUM(CASE WHEN no='1' THEN 1 ELSE 0 END) ELSE 0 END AS 미검토
        , CASE WHEN 과제검토구분='01' AND (과제진행상태구분='05' OR (과제진행상태구분='03' AND 피드백발송여부=0)) THEN SUM(CASE WHEN no='1' THEN 1 ELSE 0 END) ELSE 0 END AS 현행유지
        , CASE WHEN 과제검토구분='02' AND (과제진행상태구분='05' OR (과제진행상태구분='03' AND 피드백발송여부=0)) THEN SUM(CASE WHEN no='1' THEN 1 ELSE 0 END) ELSE 0 END AS 개선예정
        , CASE WHEN 과제검토구분='03' AND (과제진행상태구분='05' OR (과제진행상태구분='03' AND 피드백발송여부=0)) THEN SUM(CASE WHEN no='1' THEN 1 ELSE 0 END) ELSE 0 END AS 개선불가
    FROM A
    LEFT JOIN (SELECT DISTINCT 부점번호, 부서명내용, 그룹내용 FROM INST1.TSCCVCI11 WHERE 그룹회사코드='KB0') B ON A.부점번호=B.부점번호
    GROUP BY 기준년월, 응답완료년월일, 채널, 고객경험단계, 서비스품질요소, A.부점번호, 개선조치검토ID
), C AS (
    SELECT
          채널
        , '합계' AS 고객경험단계
        , '' AS 부서명
        , SUM(배분) AS 배분
        , SUM(검토완료) AS 검토
        , ROUND(SUM(검토완료) * 100.0 / SUM(배분), 1) AS 검토율
        , SUM(현행유지) AS 현행유지
        , ROUND(SUM(현행유지) * 100.0 / SUM(검토완료), 1) AS 현행유지비율
        , SUM(개선불가) AS 개선불가
        , ROUND(SUM(개선불가) * 100.0 / SUM(검토완료), 1) AS 개선불가비율
        , SUM(개선예정) AS 개선예정
        , ROUND(SUM(개선예정) * 100.0 / SUM(검토완료), 1) AS 개선예정비율
    FROM B
    WHERE 기준년월 BETWEEN '{yyyymm_cx_manage}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2, 3
    UNION ALL
    SELECT
          채널
        , 고객경험단계
        , 부서명
        , SUM(배분) AS 배분
        , SUM(검토완료) AS 검토
        , ROUND(SUM(검토완료) * 100.0 / SUM(배분), 1) AS 검토율
        , SUM(현행유지) AS 현행유지
        , ROUND(SUM(현행유지) * 100.0 / SUM(검토완료), 1) AS 현행유지비율
        , SUM(개선불가) AS 개선불가
        , ROUND(SUM(개선불가) * 100.0 / SUM(검토완료), 1) AS 개선불가비율
        , SUM(개선예정) AS 개선예정
        , ROUND(SUM(개선예정) * 100.0 / SUM(검토완료), 1) AS 개선예정비율
    FROM B
    WHERE 기준년월 BETWEEN '{yyyymm_cx_manage}' AND '{yyyymm_b01m}'
    GROUP BY 1, 2, 3
)
SELECT
      채널
    , 고객경험단계
    , 부서명
    , 배분
    , 검토
    , 검토율
    , 현행유지
    , 현행유지비율
    , 개선불가
    , 개선불가비율
    , 개선예정
    , 개선예정비율
FROM C