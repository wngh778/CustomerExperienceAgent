select 
	t.응답완료년월일, 
	t.설문참여대상자고유ID,
	t.문항ID,
	t.직원번호,
	t.소속부점코드,
	t.부점명,
	t.지역그룹관리코드,
	t.지역그룹관리구분명,
	t.지역본부합산조직코드,
	t.지역본부합산조직명,
	t.조사채널,
	t.CX코드,
	t.고객경험단계,
	t.SQ코드,
	t.서비스품질요소,
	t.고객경험VOC내용,
	t.고객감정,
	t.voc유형구분,
	t.그룹내용,
	case
		when t.업무구분 = 6 then t.개선부서
		else g.개선부서
	end as 개선부서,
	case
		when t.업무구분 <> 6 then '배분'
		else '미배분'
	end as 배분여부,
	case
		when t.검토구분 = '01' then '현행유지'
		when t.검토구분 = '02' then '개선예정'
		when t.검토구분 = '03' then '개선불가'
	end as 검토구분,
	t.검토년월일,
	case
		when t.업무구분 in (3,4,5) then '검토'
		when t.검토여부 = 1 then '검토'
		when t.검토여부 = 2 then '미검토'
		else null
	end as 검토여부,
	t.부서검토의견,
	t.과제추진사업내용,
	case -- *신규추가(업무구분)
		when t.업무구분 = 1 then '미처리'
		when t.업무구분 = 2 then '검토기한만료'
		when t.업무구분 = 3 then '검토중'
		when t.업무구분 = 4 then '승인대기'
		when t.업무구분 = 5 then '승인완료'
		when t.업무구분 = 6 then '미배분'
	end as 업무구분
from (
	select *
	from (
		select
			A.응답완료년월일 as 응답완료년월일,
			b3.문항ID as 문항ID,
			A.설문참여대상자고유ID as 설문참여대상자고유ID,
			if(A.설문조사대상구분 = '07', sv22.직원번호, null) as 직원번호,
			if(A.설문조사대상구분 = '07', sv22.소속부점코드, null) as 소속부점코드,
			ci08.부점명 as 부점명,
			ci08.지역그룹관리코드 as 지역그룹관리코드,
			if(ci08.지역그룹관리코드 = '0000', '본부직할', replace(ci08.지역그룹관리구분명, '·', '')) as 지역그룹관리구분명,
			ci08.지역본부합산조직코드 as 지역본부합산조직코드,
			if(ci08.지역본부합산조직코드 = '0000', '본부직할', replace(ci08.지역본부합산조직명, '·', '')) as 지역본부합산조직명,
			b3.조사채널내용 as 조사채널,
			b3.고객경험단계코드 as CX코드,
			b3.고객경험단계내용 as 고객경험단계,
			b3.서비스품질요소코드 as SQ코드,
			b3.인스턴스내용 as 서비스품질요소,
			b3.문항응답내용 as 고객경험VOC내용,
		    CASE
		        WHEN b3.고객감정대분류구분 = '01' THEN '긍정'
		        WHEN b3.고객감정대분류구분 = '02' THEN '부정'
		        WHEN b3.고객감정대분류구분 = '03' THEN '중립'
		    END AS 고객감정,
		    case
		        WHEN b3.고객경험VOC유형구분 = '01' THEN '칭찬'
		        WHEN b3.고객경험VOC유형구분 = '02' THEN '불만'
		        WHEN b3.고객경험VOC유형구분 = '03' THEN '개선'
		        WHEN b3.고객경험VOC유형구분 = '99' THEN '기타'
		    END AS voc유형구분,
			b3.관리그룹내용 as 그룹내용,
			b3.개선부점내용 as 개선부서,
			A.개선부서분배여부 as 배분여부,
			D.과제검토구분 as 검토구분,
			substr(B.작성년월일시, 1, 8) as 검토년월일,
			b3.검토여부 as 검토여부,
			b3.검토기한년월일,
			b3.작성년월일,
			D.과제검토의견내용 as 부서검토의견,
			D.과제추진사업내용 as 과제추진사업내용,
			case 
				when b3.개선부서분배여부=1
					and A.개선부서분배여부 = 1
					and b3.검토여부=0
					and C.과제진행상태구분 is null
					and D.과제검토구분 is null
					and b3.검토기한년월일 >= b3.작성년월일
					and B.설문참여대상자고유ID is null
						then 1 -- 미처리
				when b3.개선부서분배여부=1
					and b3.검토여부=0
					and b3.검토기한년월일 < b3.작성년월일
					and A.개선부서분배여부 =1
					and A.직원제공여부 = 1
					and A.부점장제공여부 = 1
						then 2 -- 검토기한만료
				when b3.개선부서분배여부=1
					and A.개선부서분배여부 = 1
					and b3.검토여부=0
					and D.과제진행상태구분 = '01'
						then 3 -- 검토중
				when b3.개선부서분배여부=1
					and C.과제진행상태구분 = '02'
						then 4 -- 승인대기
				when b3.개선부서분배여부=1
					and D.과제검토구분 is not null
					and C.과제진행상태구분 = '03'
						then 5 -- 실시간승인완료
				else 6 -- 미배분
			end
				as 업무구분
	from
		inst1.tsccvmgb3 b3
	left join inst1.tsccvmg27 A on
		1=1
		and b3.그룹회사코드 = A.그룹회사코드
		and b3.설문참여대상자고유ID = A.설문참여대상자고유ID
		and b3.문항ID = A.문항ID
		and b3.문항설문조사대상구분 = A.설문조사대상구분
	    LEFT JOIN inst1.tsccvmg84 B ON A.그룹회사코드 = B.그룹회사코드 AND A.설문ID = B.설문ID AND A.문항ID = B.문항ID AND A.설문참여대상자고유ID = B.설문참여대상자고유ID 
	    						AND A.순서일련번호 = B.순서일련번호 AND A.응답완료년월일 = B.응답완료년월일
	    LEFT JOIN inst1.tsccvmg81 C ON B.그룹회사코드 = C.그룹회사코드 AND B.설문ID = C.설문ID AND B.개선조치검토ID = C.개선조치검토ID
	    LEFT JOIN inst1.tsccvmg82 D ON C.그룹회사코드 = D.그룹회사코드 AND C.설문ID = D.설문ID AND C.개선조치검토ID = D.개선조치검토ID AND D.최종데이터여부 = '1'
		LEFT JOIN inst1.tsccvsv22 sv22 ON A.그룹회사코드 = sv22.그룹회사코드 
			and A.설문ID = sv22.설문ID and  A.설문참여대상자고유ID = sv22.설문참여대상자고유ID
		LEFT JOIN inst1.tsccvci08 ci08 ON A.그룹회사코드 = ci08.그룹회사코드 
			and A.응답완료년월일 = ci08.기준년월일 and  sv22.소속부점코드 = ci08.부점코드
	where 1=1 
		and sv22.그룹회사코드 = 'KB0'
		and b3.설문응답종료년월일 >= '{yyyy0101}'
		and b3.기준년월일 >= '{yyyy0101}'
		and A.관리설정여부=1
	) x
	where x.업무구분 <> 6
	union all
	select *
	from (
		select
			A.기준년월일 as 응답완료년월일,
			b3.문항ID as 문항ID,
			A.설문참여대상자고유ID as 설문참여대상자고유ID,
			if(A.문항설문조사대상구분 = '07', sv22.직원번호, null) as 직원번호,
			if(A.문항설문조사대상구분 = '07', sv22.소속부점코드, null) as 소속부점코드,
			ci08.부점명 as 부점명,
			ci08.지역그룹관리코드 as 지역그룹관리코드,
			if(ci08.지역그룹관리코드 = '0000', '본부직할', replace(ci08.지역그룹관리구분명, '·', '')) as 지역그룹관리구분명,
			ci08.지역본부합산조직코드 as 지역본부합산조직코드,
			if(ci08.지역본부합산조직코드 = '0000', '본부직할', replace(ci08.지역본부합산조직명, '·', '')) as 지역본부합산조직명,
			b3.조사채널내용 as 조사채널,
			b3.고객경험단계코드 as CX코드,
			b3.고객경험단계내용 as 고객경험단계,
			b3.서비스품질요소코드 as SQ코드,
			b3.인스턴스내용 as 서비스품질요소,
			b3.문항응답내용 as 고객경험VOC내용,
		    CASE
		        WHEN b3.고객감정대분류구분 = '01' THEN '긍정'
		        WHEN b3.고객감정대분류구분 = '02' THEN '부정'
		        WHEN b3.고객감정대분류구분 = '03' THEN '중립'
		    END AS 고객감정,
		    case
		        WHEN b3.고객경험VOC유형구분 = '01' THEN '칭찬'
		        WHEN b3.고객경험VOC유형구분 = '02' THEN '불만'
		        WHEN b3.고객경험VOC유형구분 = '03' THEN '개선'
		        WHEN b3.고객경험VOC유형구분 in ('99') THEN '기타'
		        WHEN b3.고객경험VOC유형구분 not in ('01', '02', '03') THEN '기타'
		    END AS voc유형구분,
			b3.관리그룹내용 as 그룹내용,
			b3.개선부점내용 as 개선부서,
			A.개선부서분배여부 as 배분여부,
			D.과제검토구분 as 검토구분,
			b3.검토년월일 as 검토년월일,
			b3.검토여부 as 검토여부,
			b3.검토기한년월일,
			b3.작성년월일,
			b3.과제검토의견내용 as 부서검토의견,
			b3.과제추진사업내용 as 과제추진사업내용,
			case 
				when b3.개선부서분배여부=1
					and A.개선부서분배여부 = 1
					and A.관리설정여부 = 1
					and b3.검토여부=0
					and C.과제진행상태구분 is null
					and D.과제검토구분 is null
					and b3.검토기한년월일 >= b3.작성년월일
					and B.설문참여대상자고유ID is null
						then 1 -- 미처리
				when b3.개선부서분배여부=1
					and b3.검토여부=0
					and b3.검토기한년월일 < b3.작성년월일
					and A.개선부서분배여부 =1
					and A.직원제공여부 = 1
					and A.부점장제공여부 = 1
						then 2 -- 검토기한만료
				when b3.개선부서분배여부=1
					and A.개선부서분배여부 = 1
					and b3.검토여부=0
					and D.과제진행상태구분 = '01'
						then 3 -- 검토중
				when b3.개선부서분배여부=1
					and C.과제진행상태구분 = '02'
						then 4 -- 승인대기
				when b3.개선부서분배여부=1
					and D.과제검토구분 is not null
					and C.과제진행상태구분 = '03'
						then 5 -- 실시간승인완료
				else 6 -- 미배분
			end
				as 업무구분
	from
		inst1.tsccvmgb3 b3
	left join inst1.tsccvmg57 A on
		1=1
		and b3.그룹회사코드 = A.그룹회사코드
		and b3.설문참여대상자고유ID = A.설문참여대상자고유ID
		and b3.설문응답종료년월일 = A.기준년월일
		and b3.문항ID = A.문항ID
		and b3.문항설문조사대상구분 = A.문항설문조사대상구분
	    LEFT JOIN inst1.tsccvmg84 B ON A.그룹회사코드 = B.그룹회사코드 AND A.설문ID = B.설문ID AND A.문항ID = B.문항ID AND A.설문참여대상자고유ID = B.설문참여대상자고유ID 
	    						AND A.정렬순서 = B.순서일련번호 AND A.기준년월일 = B.응답완료년월일
	    LEFT JOIN inst1.tsccvmg81 C ON B.그룹회사코드 = C.그룹회사코드 AND B.설문ID = C.설문ID AND B.개선조치검토ID = C.개선조치검토ID
	    LEFT JOIN inst1.tsccvmg82 D ON C.그룹회사코드 = D.그룹회사코드 AND C.설문ID = D.설문ID AND C.개선조치검토ID = D.개선조치검토ID AND D.최종데이터여부 = '1'
		LEFT JOIN inst1.tsccvsv22 sv22 ON A.그룹회사코드 = sv22.그룹회사코드 
			and A.설문ID = sv22.설문ID and  A.설문참여대상자고유ID = sv22.설문참여대상자고유ID
		LEFT JOIN inst1.tsccvci08 ci08 ON A.그룹회사코드 = ci08.그룹회사코드 
			and A.기준년월일 = ci08.기준년월일 and  sv22.소속부점코드 = ci08.부점코드
	where 1=1 
		and sv22.그룹회사코드 = 'KB0'
		and b3.설문응답종료년월일 >= '{yyyy0101}'
		and b3.기준년월일 >= '{yyyy0101}'
	) y
	where y.업무구분 = 6
	group by y.응답완료년월일, y.설문참여대상자고유ID, y.조사채널, y.문항ID
) t
left join (
	select 
		A.설문참여대상자고유ID,
		min(F.부서명내용) as 개선부서
	from inst1.tsccvmg27 A
	left join inst1.tsccvci11 F
		on 1=1
		and A.회계부점코드 = F.회계부점코드
		and A.부점번호 = F.부점번호
	where A.개선부서분배여부 = 1
		and F.부서명내용 is not null
	group by A.설문참여대상자고유ID
) g
	on 1=1 and t.설문참여대상자고유ID = g.설문참여대상자고유ID
where 조사채널 = '영업점' and 응답완료년월일 between '{yyyy0101}' and '{friday_b01w}'
group by t.응답완료년월일, t.설문참여대상자고유ID, t.문항ID, t.조사채널
ORDER by 응답완료년월일, 설문참여대상자고유ID, 조사채널, 고객경험단계, 서비스품질요소
