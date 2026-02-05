WITH SQ AS (
	SELECT DISTINCT 
                설문조사방식구분, 설문조사종류구분, 설문조사대상구분, 서비스품질요소코드, 서비스품질요소명 
        FROM INST1.TSCCVCI07
        WHERE 사용여부=1
), MG27 AS (
    SELECT
        그룹회사코드
        , 고객식별자
        , 설문ID
        , 문항ID
        , 설문참여대상자고유ID
        , 응답완료년월일
        , 기준년월
        , 순서일련번호
        , CASE
            WHEN 고객경험단계구분 IN ('07', '08', '09', '10', '11', '12') THEN "09"
            ELSE 설문조사대상구분
        END AS 설문조사대상구분
        , 고객경험단계구분
        , 서비스품질요소코드
        , 부점번호
        , 회계부점코드
        , 고객감정대분류구분
        , 고객경험VOC유형구분
        , 에피소드유형구분
    FROM 
        (
            SELECT
                A.*
                , ROW_NUMBER() OVER (
                PARTITION BY 그룹회사코드, 고객식별자, 
                    설문ID, 문항ID, 설문참여대상자고유ID, 응답완료년월일
                ORDER BY 순서일련번호 DESC -- 주로 상품 쪽이 후순위 정렬 대상.
               ) AS 분류결과번호
            FROM INST1.TSCCVMG27 A
            WHERE -- 상품에 관한 결과가 KB 스타뱅킹, 영업점에 중복됨.                
                설문조사대상구분 = "09" OR 고객경험단계구분 NOT IN ('07', '08', '09', '10', '11', '12') 
        ) A
    WHERE 1=1
        AND 기준년월 >= '202503' AND 응답완료년월일 >= '20250301'
        AND 관리설정여부 >= 1
        AND 개선부서분배여부 = 1
        AND 질문의도대구분 = '09' -- 개선의견은 배분대상에 포함되지 않는 것이 맞는지?
        AND 설문조사대상구분 IS NOT NULL
        AND 고객경험단계구분 IS NOT NULL
        AND 서비스품질요소코드={cxc}
        AND 분류결과번호 = 1 -- 중복된 데이터 떄문에 순서일련번호를 추가적으로 PK로 설정했었던 것으로 보임. UNIQUENESS 충족을 위해 조건 추가. 
        AND 기준년월 >= '{prev_year}01'
), RELEVANT_VOC AS (
    SELECT
        A.응답완료년월일 -- PK
        , A.기준년월
        , A.설문ID -- PK
        , A.문항ID -- PK
        , A.설문참여대상자고유ID -- PK
        , A.설문조사대상구분
        , A.고객경험단계구분
        , J.문항응답내용
        , A.서비스품질요소코드
        , B.개선조치검토ID
        , A.부점번호
        , A.회계부점코드
        , "부서명내용" AS 개선부서 -- , DP.부서명내용 AS 부서명. 마스킹.
        , DP.그룹내용 AS 개선사업그룹
        , COALESCE(N5.인스턴스내용, "미처리") AS 진행상태
        , COALESCE(N6.인스턴스내용, "미처리") AS 검토구분
        , LEFT(B.시스템최종처리일시, 8) AS 검토년월일
        , D.과제검토배경내용
        , D.과제운영현황내용
        , D.과제검토의견내용
        , D.과제조치계획내용
        , D.과제추진사업내용
        , D.과제반려사유내용
        , D.과제피드백의견내용
        , D.개선이행시작년월일
        , D.개선이행종료년월일
        , N7.인스턴스내용 AS 피드백발송여부
    FROM MG27 A
    -- JOIN하는 테이블은 모두 PK에 대해 JOIN하도록 하여 여러 행이 매칭되는 것을 방지함.
    LEFT JOIN INST1.TSCCVMG84 B ON 
        A.그룹회사코드 = B.그룹회사코드 AND 
        A.설문ID = B.설문ID AND 
        A.문항ID = B.문항ID AND 
        A.응답완료년월일 = B.응답완료년월일 AND
        A.설문참여대상자고유ID = B.설문참여대상자고유ID AND 
        A.순서일련번호 = B.순서일련번호
    LEFT JOIN INST1.TSCCVMG81 C ON 
        B.그룹회사코드 = C.그룹회사코드 AND 
        B.설문ID = C.설문ID AND 
        B.개선조치검토ID = C.개선조치검토ID
    LEFT JOIN (
        SELECT 
            TMP.*
            , ROW_NUMBER() OVER (
                PARTITION BY 그룹회사코드, 설문ID, 개선조치검토ID ORDER BY 일련번호 DESC
            ) AS 입력순서 -- 과제 검토 사항에 수정이 있는 경우 일련번호가 추가되므로 DESC
        FROM INST1.TSCCVMG82 TMP
        WHERE 과제검토구분 IS NOT NULL
        ) D ON 
        B.그룹회사코드 = D.그룹회사코드 AND 
        B.설문ID = D.설문ID AND 
        B.개선조치검토ID = D.개선조치검토ID AND 
        D.입력순서 = 1 AND -- PK가 일련번호이므로 이 방식으로 UNIQUENESS 확정. (최종데이터여부='1' 또한 같은 목적으로 추측됨.)
        TRIM(D.과제검토의견내용) <> ""
    LEFT JOIN INST1.TSCCVMGA4 E ON 
        A.그룹회사코드 = E.그룹회사코드 AND 
        A.서비스품질요소코드 = E.서비스품질요소코드 AND 
        A.에피소드유형구분 = E.에피소드유형구분 AND 
        E.일련번호 = 1 AND
        E.사용여부 = '1'
    LEFT JOIN INST1.TSCCVCI12 F ON
        A.응답완료년월일 = F.기준년월일
    LEFT JOIN INST1.VSCCVSV73 J ON 
        A.그룹회사코드 = J.그룹회사코드 AND 
        A.응답완료년월일 = J.설문응답종료년월일 AND 
        A.설문ID = J.설문ID AND 
        A.설문참여대상자고유ID = J.설문참여대상자고유ID AND 
        A.문항ID = J.문항ID AND 
        J.문항선택항목ID = "" AND 
        J.문항구분 = '01' AND
        J.질문의도대구분 = '09'
    LEFT JOIN INST1.TSCCVCI11 DP ON DP.그룹회사코드="KB0" AND A.부점번호=DP.부점번호
    LEFT JOIN INST1.TSCCVCI04 N5 ON N5.그룹회사코드="KB0" AND C.과제진행상태구분=N5.인스턴스코드 AND N5.인스턴스식별자='142680000'
    LEFT JOIN INST1.TSCCVCI04 N6 ON N6.그룹회사코드="KB0" AND D.과제검토구분=N6.인스턴스코드 AND N6.인스턴스식별자='142679000'
    LEFT JOIN INST1.TSCCVCI04 N7 ON N7.그룹회사코드="KB0" AND D.피드백발송여부=N7.인스턴스코드 AND N7.인스턴스식별자='102132000'
)
SELECT * FROM RELEVANT_VOC WHERE {keywords_condition} AND 검토구분<>"미처리" ORDER BY 응답완료년월일 DESC LIMIT {limit_len}