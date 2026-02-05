SELECT
      mid.고객경험정답감정코드
    , mid.고객경험정답감정명
    , mid.고객감정대분류구분
    , large.인스턴스내용 AS 고객감정대분류구분명
    , mid.고객경험VOC유형구분
    , vocType.인스턴스내용 AS 고객경험VOC유형구분명
FROM inst1.TSCCVMG88 mid
    INNER JOIN inst1.TSCCVCI04 large
         ON large.그룹회사코드 = mid.그룹회사코드
        AND large.인스턴스식별자 = '142528000'
        AND large.인스턴스코드 = mid.고객감정대분류구분
    INNER JOIN inst1.TSCCVCI04 vocType
         ON vocType.그룹회사코드 = mid.그룹회사코드
        AND vocType.인스턴스식별자 = '142529000'
        AND vocType.인스턴스코드 = mid.고객경험VOC유형구분
WHERE mid.그룹회사코드 = 'KB0'
  AND mid.사용여부 = '1'
;