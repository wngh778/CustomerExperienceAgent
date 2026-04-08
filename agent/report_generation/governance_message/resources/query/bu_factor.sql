select case
 	 	when D.인스턴스내용 = '스타뱅킹' then 'KB 스타뱅킹'
 	 	else D.인스턴스내용
 	   end as 채널
	 , TRIM(B.인스턴스내용) AS 고객경험단계
 	 , C.서비스품질요소명 as 서비스품질요소 
 	 , sum(A.인입VOC건수) as 전체건수
 	 , A.긍정건수
 	 , round(sum(A.긍정건수)/sum(A.인입VOC건수) *100,1) as 긍정비율
 	 , A.부정건수
 	 , round(sum(A.부정건수)/sum(A.인입VOC건수) *100,1) as 부정비율 
from inst1.TSCCVMGB4 A 
JOIN INST1.TSCCVCI04 B ON B.그룹회사코드 = 'KB0' AND B.인스턴스식별자 = '142594000' AND A.고객경험단계구분 = B.인스턴스코드 -- 고객경험단계명
join ( 
	    SELECT DISTINCT  
	           서비스품질요소코드 
	         , 서비스품질요소명 
	      FROM INST1.TSCCVCI07
	      where 그룹회사코드 = 'KB0' 
	      and 설문조사방식구분 = '02' 
) C on A.서비스품질요소코드 = C.서비스품질요소코드 
JOIN INST1.TSCCVCI04 D ON D.그룹회사코드 = 'KB0' AND D.인스턴스식별자 = '142668000' and A.관리지표채널구분 = D.인스턴스코드 -- 채널명
where A.그룹회사코드 = 'KB0' 
and D.인스턴스내용 = REPLACE('{{channel_type}}', 'KB 스타뱅킹', '스타뱅킹')
and (A.기준년월, A.기준년월일) in (select distinct 기준년월, max(기준년월일) over (partition by 기준년월) as 기준년월일  
							 from inst1.TSCCVMGB4 
							 WHERE 그룹회사코드 = 'KB0' 
							 AND NPS업무구분 = '00' 
							)   
and A.NPS업무구분 = '03' 
and A.인입VOC건수 != 0 
and A.기준년월 like CONCAT(SUBSTRING('{{base_date}}', 1, 4), '%') 
group by 채널, 고객경험단계, 서비스품질요소
;
