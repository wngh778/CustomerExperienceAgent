select 
		HQ.지역본부합산조직명 as 지역본부
		, MAIN.고객경험단계명 as 고객경험단계 
		, case 
		when KK.preMon_Cmpr is null then '-' 
		when KK.preMon_Cmpr=1 then '우수' 
		when KK.preMon_Cmpr=2 then '양호' 
		when KK.preMon_Cmpr=3 then '개선' 
		else KK.preMon_Cmpr end as 전월PGNPS
		, case 
		when KK.nowMon_Cmpr is null then '-' 
		when KK.nowMon_Cmpr=1 then '우수' 
		when KK.nowMon_Cmpr=2 then '양호' 
		when KK.nowMon_Cmpr=3 then '개선' 
		else KK.nowMon_Cmpr end as 당월PGNPS		
		, case when MAIN.고객경험단계구분 = '99' then nowMon_Cmpr - preMon_Cmpr
		else case when preMon_Cmpr < nowMon_Cmpr then '하락'
			when preMon_Cmpr = nowMon_Cmpr then '유지'
			when preMon_Cmpr > nowMon_Cmpr then '상승'
		else '-' end end as PGNPS변동
	, case 
		when KK.1month_Cmpr is null then '-' 
		when KK.1month_Cmpr=1 then '우수' 
		when KK.1month_Cmpr=2 then '양호' 
		when KK.1month_Cmpr=3 then '개선' 
		else KK.1month_Cmpr end as 1월PGNPS
	, case 
		when KK.2month_Cmpr is null then '-' 
		when KK.2month_Cmpr=1 then '우수' 
		when KK.2month_Cmpr=2 then '양호' 
		when KK.2month_Cmpr=3 then '개선' 
		else KK.2month_Cmpr end as 2월PGNPS
	, case 
		when KK.3month_Cmpr is null then '-' 
		when KK.3month_Cmpr=1 then '우수' 
		when KK.3month_Cmpr=2 then '양호' 
		when KK.3month_Cmpr=3 then '개선' 
		else KK.3month_Cmpr end as 3월PGNPS
	, case 
		when KK.4month_Cmpr is null then '-' 
		when KK.4month_Cmpr=1 then '우수' 
		when KK.4month_Cmpr=2 then '양호' 
		when KK.4month_Cmpr=3 then '개선' 
		else KK.4month_Cmpr end as 4월PGNPS
	, case 
		when KK.5month_Cmpr is null then '-' 
		when KK.5month_Cmpr=1 then '우수' 
		when KK.5month_Cmpr=2 then '양호' 
		when KK.5month_Cmpr=3 then '개선' 
		else KK.5month_Cmpr end as 5월PGNPS
	, case 
		when KK.6month_Cmpr is null then '-' 
		when KK.6month_Cmpr=1 then '우수' 
		when KK.6month_Cmpr=2 then '양호' 
		when KK.6month_Cmpr=3 then '개선' 
		else KK.6month_Cmpr end as 6월PGNPS
	, case 
		when KK.7month_Cmpr is null then '-' 
		when KK.7month_Cmpr=1 then '우수' 
		when KK.7month_Cmpr=2 then '양호' 
		when KK.7month_Cmpr=3 then '개선' 
		else KK.7month_Cmpr end as 7월PGNPS
	, case 
		when KK.8month_Cmpr is null then '-' 
		when KK.8month_Cmpr=1 then '우수' 
		when KK.8month_Cmpr=2 then '양호' 
		when KK.8month_Cmpr=3 then '개선' 
		else KK.8month_Cmpr end as 8월PGNPS
	, case 
		when KK.9month_Cmpr is null then '-' 
		when KK.9month_Cmpr=1 then '우수' 
		when KK.9month_Cmpr=2 then '양호' 
		when KK.9month_Cmpr=3 then '개선' 
		else KK.9month_Cmpr end as 9월PGNPS
	, case 
		when KK.10month_Cmpr is null then '-' 
		when KK.10month_Cmpr=1 then '우수' 
		when KK.10month_Cmpr=2 then '양호' 
		when KK.10month_Cmpr=3 then '개선' 
		else KK.10month_Cmpr end as 10월PGNPS
	, case 
		when KK.11month_Cmpr is null then '-' 
		when KK.11month_Cmpr=1 then '우수' 
		when KK.11month_Cmpr=2 then '양호' 
		when KK.11month_Cmpr=3 then '개선' 
		else KK.11month_Cmpr end as 11월PGNPS
	, case 
		when KK.12month_Cmpr is null then '-' 
		when KK.12month_Cmpr=1 then '우수' 
		when KK.12month_Cmpr=2 then '양호' 
		when KK.12month_Cmpr=3 then '개선' 
		else KK.12month_Cmpr end as 12월PGNPS
from (
		SELECT RTRIM(H.인스턴스코드) as 고객경험단계구분
				, case when RTRIM(H.인스턴스코드) = '00' then '채널'
					else H.인스턴스내용 end as 고객경험단계명
		FROM inst1.tsccvci04 H 
		WHERE H.그룹회사코드='KB0'
		and H.인스턴스식별자='142594000'
		and H.인스턴스코드 in ('00', '03')   
)MAIN		
left outer join 
(
	WITH initData as ( -- 기초 날짜 추출
		select baseYmd 
				, substr(baseYmd,1,4) as baseYear
				, date_format(date_add(baseYmd, interval -1 MONTH), '%Y%m') as preMon
				, date_format(baseYmd, '%Y%m') as thisMon
		from(
			select '{friday_b01w}' as baseYmd /*여기!!!!!!!!!!!!*/
			from dual
		)A				
	), group_cd as ( -- 지역그룹 관리코드 추출
		select 지역그룹관리코드
		from inst1.tsccvmga5 A
		where 그룹회사코드 = 'KB0'
		and substr(응답완료년월일,1,4) = (select baseYear from initData)
		and 고객경험단계구분 = '00'
		group by 지역그룹관리코드
	), Dvsn_Cnfs_preMon as ( -- 이전월까지의 평균혼잡도로 통한 부점별 혼잡도군 추출
			select 그룹회사코드
					, 부점코드
					, avg(고객혼잡지수) as 고객혼잡지수
					, case when 70 <= avg(고객혼잡지수) then '01'
						when 60 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 70 then '02'
						when 40 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 60 then '03'
						when avg(고객혼잡지수) < 40  then '04'
						else '04'end 혼잡도군
			from inst1.tsccvci15
			where 그룹회사코드 = 'KB0'
			and substr(기준년월,1,4) = (select baseYear from initData)
			and 기준년월 <= (select preMon from initData)
			group by 그룹회사코드, 부점코드
	), Dvsn_Cnfs_thisMon as ( -- 해당월까지의 평균혼잡도로 통한 부점별 혼잡도군 추출
			select 그룹회사코드
					, 부점코드
					, avg(고객혼잡지수) as 고객혼잡지수
					, case when 70 <= avg(고객혼잡지수) then '01'
						when 60 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 70 then '02'
						when 40 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 60 then '03'
						when avg(고객혼잡지수) < 40  then '04'
						else '04'end 혼잡도군
			from inst1.tsccvci15
			where 그룹회사코드 = 'KB0'
			and substr(기준년월,1,4) = (select baseYear from initData)
			and 기준년월 <= (select thisMon from initData)
			group by 그룹회사코드, 부점코드
	), Dvsn_Cnfs_month as ( -- 부점별 월별 혼잡도군 추출
			select 그룹회사코드
					, 부점코드
					, 기준년월
					, 고객혼잡지수
					, case when 70 <= 고객혼잡지수 then '01'
						when 60 <= 고객혼잡지수 and 고객혼잡지수 < 70 then '02'
						when 40 <= 고객혼잡지수 and 고객혼잡지수 < 60 then '03'
						when 고객혼잡지수 < 40  then '04'
						else '04'end 혼잡도군
			from inst1.tsccvci15
			where 그룹회사코드 = 'KB0'
			and substr(기준년월,1,4) = (select baseYear from initData)
	), All_CXNPS AS ( -- 지역영업그룹 혼잡도군별 월별 NPS점수(월별기준)
			select 기준년월
					, 지역그룹관리코드
					, 혼잡도군
					, 고객경험단계구분
					, (sum(case when NPS점수 >= 9 then 1 else 0 end ) - sum(case when NPS점수 <= 6 then 1 else 0 end ))/count(*)*100 as CXNPS 
			from( /*서브쿼리 : 지역영업그룹-지점-직원 혼잡도 및 NPS 점수 RAW*/
				select A.고객경험단계구분 as 고객경험단계구분
						, A.지역그룹관리코드
						, B.고객혼잡지수 -- 헤딩 지점의 혼잡지수
						, B.혼잡도군
						, substr(A.응답완료년월일,1,6) as 기준년월
						, A.NPS점수 
				from inst1.tsccvmga5 A
				left join  Dvsn_Cnfs_month B on A.그룹회사코드 = B.그룹회사코드 and A.부점코드 = B.부점코드 and substr(A.응답완료년월일,1,6) = B.기준년월
				where substr(A.응답완료년월일,1,4) = (select baseYear from initData)
				and A.응답완료년월일 <= (select baseYmd from initData)
				and A.고객경험단계구분 in ('00', '03')   
				and A.지역그룹관리코드 is not null
				and 지역그룹관리코드 in (select 지역그룹관리코드 from group_cd)
			)A
			group by 기준년월, 고객경험단계구분, 지역그룹관리코드, 혼잡도군
			order by 지역그룹관리코드, 기준년월 desc, 혼잡도군, 고객경험단계구분
	), All_CXNPS_PREMON AS (
			select 지역그룹관리코드
					, 혼잡도군
					, 고객경험단계구분
					, (sum(case when NPS점수 >= 9 then 1 else 0 end ) - sum(case when NPS점수 <= 6 then 1 else 0 end ))/count(*)*100 as CXNPS 
			from(
				select A.고객경험단계구분 as 고객경험단계구분
						, A.지역그룹관리코드
						, ifnull(B.혼잡도군,'04') as 혼잡도군
						, substr(A.응답완료년월일,1,6) as 기준년월
						, A.NPS점수 
				from inst1.tsccvmga5 A
				left join  Dvsn_Cnfs_preMon B on A.그룹회사코드 = B.그룹회사코드 and A.부점코드 = B.부점코드
				where substr(A.응답완료년월일,1,4) = (select baseYear from initData)
				and substr(A.응답완료년월일,1,6) <= (select preMon from initData)
				and A.고객경험단계구분 in ('00', '03')   
				and A.지역그룹관리코드 is not null
				and 지역그룹관리코드 in (select 지역그룹관리코드 from group_cd)
		)A
		group by 고객경험단계구분, 지역그룹관리코드, 혼잡도군
	), All_CXNPS_THISMON AS ( -- 지역영업그룹 혼잡도군별 당월누적 NPS점수(누적기준)
				select 지역그룹관리코드
						, 혼잡도군
						, 고객경험단계구분
						, (sum(case when NPS점수 >= 9 then 1 else 0 end ) - sum(case when NPS점수 <= 6 then 1 else 0 end ))/count(*)*100 as CXNPS 
				from(
					select A.고객경험단계구분 as 고객경험단계구분
							, A.지역그룹관리코드
							, ifnull(B.혼잡도군,'04') as 혼잡도군
							, substr(A.응답완료년월일,1,6) as 기준년월
							, A.NPS점수 
					from inst1.tsccvmga5 A
					left join  Dvsn_Cnfs_thisMon B on A.그룹회사코드 = B.그룹회사코드 and A.부점코드 = B.부점코드
					where substr(A.응답완료년월일,1,4) = (select baseYear from initData)
					and substr(A.응답완료년월일,1,6) <= (select thisMon from initData)
					and A.응답완료년월일 <= (select baseYmd from initData)
					and A.고객경험단계구분 in ('00', '03') 
					and A.지역그룹관리코드 is not null
					and 지역그룹관리코드 in (select 지역그룹관리코드 from group_cd)
			)A
			group by 고객경험단계구분, 지역그룹관리코드, 혼잡도군
		), CXNPS AS ( -- 지역영업그룹 지역본부 혼잡도군별 월별 NPS 점수
			select 설문응답년월
					, 지역그룹관리코드
					, 지역본부합산조직코드
					, avg(고객혼잡지수) as 고객혼잡지수
					, case when 70 <= avg(고객혼잡지수) then '01'
									when 60 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 70 then '02'
									when 40 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 60 then '03'
									when avg(고객혼잡지수) < 40  then '04'
									else '04'end 혼잡도군
					, 고객경험단계구분
					, (sum(추천고객수) - sum(비추천고객수))/count(*)*100 as CXNPS 
					, sum(추천고객수) as 추천고객수 
					, sum(중립고객수)as 중립고객수 
					, sum(비추천고객수)as 비추천고객수 
					, count(*) as 전체응답자수
			from (
					select substr(응답완료년월일,1,6) as 설문응답년월
							, 지역그룹관리코드
							, 지역본부합산조직코드
							, 고객혼잡지수
							, 고객경험단계구분 as 고객경험단계구분
							, NPS점수 
							, 추천고객수 
							, 중립고객수 
							, 비추천고객수 
					from inst1.tsccvmga5 A
					left join  Dvsn_Cnfs_month B on A.그룹회사코드 = B.그룹회사코드 and A.부점코드 = B.부점코드 and substr(A.응답완료년월일,1,6) = B.기준년월
					where A.그룹회사코드 ='KB0'
					and A.응답완료년월일 >= (select concat( baseYear,'0101') from initData)
					and A.응답완료년월일 <= (select concat( baseYear,'1231') from initData)
					and A.응답완료년월일 <= (select baseYmd from initData)
					and A.고객경험단계구분 in ('00', '03')
					and A.지역그룹관리코드 is not null
					and A.지역본부합산조직코드 in (
							select 지역본부합산조직코드
							from inst1.tsccvmga5 A
							where 그룹회사코드 ='KB0'
							and 응답완료년월일 >= (select concat( baseYear,'0101') from initData)
							and A.응답완료년월일 <= (select baseYmd from initData)
							group by 지역그룹관리코드, 지역본부합산조직코드
					)
				)A 
				group by 설문응답년월, 지역그룹관리코드, 지역본부합산조직코드, A.고객경험단계구분
				order by 지역그룹관리코드, 지역본부합산조직코드, 설문응답년월, 고객경험단계구분
		), cal as ( -- (월별) CX_NPS(지역본부) 기준으로 혼잡도별 NPS에 따라 우수/양호/개선필요 구분
			select A.설문응답년월
					, A.지역그룹관리코드
					, A.지역본부합산조직코드
					, A.혼잡도군
					, A.고객경험단계구분
					, A.CXNPS
					, case when B.CXNPS is null then null 
							when B.CXNPS * 1.04 < A.CXNPS then 1
							when B.CXNPS * 0.96 <= A.CXNPS and A.CXNPS <= B.CXNPS * 1.04 then 2
							when A.CXNPS < B.CXNPS * 0.96 then 3
							else null end CXNPS_Cmpr
					, A.추천고객수 
					, A.중립고객수 
					, A.비추천고객수 
					, A.전체응답자수
			from CXNPS A 
			left join All_CXNPS B on A.설문응답년월 = B.기준년월 and  A.지역그룹관리코드 = B.지역그룹관리코드 and A.혼잡도군 = B.혼잡도군 and  A.고객경험단계구분 = B.고객경험단계구분
			group by 설문응답년월, A.지역그룹관리코드, A.지역본부합산조직코드, A.고객경험단계구분
			), CXNPS_ACUM AS ( -- 누적 NPS를 만들기 위한 raw (아래 테이블에서 사용)
			select 설문응답년월
					, 지역그룹관리코드
					, 지역본부합산조직코드
					, avg(고객혼잡지수) as 고객혼잡지수
					, case when 70 <= avg(고객혼잡지수) then '01'
						when 60 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 70 then '02'
						when 40 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 60 then '03'
						when avg(고객혼잡지수) < 40  then '04'
						else '04'end 혼잡도군
					, 고객경험단계구분
					, (sum(추천고객수) - sum(비추천고객수))/count(*)*100 as CXNPS 
					, sum(추천고객수) as 추천고객수 
					, sum(중립고객수)as 중립고객수 
					, sum(비추천고객수)as 비추천고객수 
					, count(*) as 전체응답자수
			from (
					select substr(응답완료년월일,1,6) as 설문응답년월
							, 지역그룹관리코드
							, 지역본부합산조직코드
							, 고객혼잡지수
							, 고객경험단계구분 as 고객경험단계구분
							, NPS점수 
							, 추천고객수 
							, 중립고객수 
							, 비추천고객수 
					from inst1.tsccvmga5 A
					left join  Dvsn_Cnfs_month B on A.그룹회사코드 = B.그룹회사코드 and A.부점코드 = B.부점코드 and substr(A.응답완료년월일,1,6) = B.기준년월
					where A.그룹회사코드 ='KB0'
					and A.응답완료년월일 >= (select concat( baseYear,'0101') from initData)
					and A.응답완료년월일 <= (select concat( baseYear,'1231') from initData)
					and substr(A.응답완료년월일,1,6) <= (select thisMon from initData)
					and A.응답완료년월일 <= (select baseYmd from initData)
					and A.고객경험단계구분 in ('00', '03')
					and A.지역그룹관리코드 is not null
					and A.지역본부합산조직코드 in (
							select 지역본부합산조직코드
							from inst1.tsccvmga5
							where 그룹회사코드 ='KB0'
							and 응답완료년월일 >= (select concat( baseYear,'0101') from initData)
							group by 지역그룹관리코드, 지역본부합산조직코드
					)
				)A 
				group by 설문응답년월, 지역그룹관리코드, 지역본부합산조직코드, A.고객경험단계구분
		), calPreMon as (
			select A.지역그룹관리코드
					, A.지역본부합산조직코드
					, A.고객경험단계구분
					, B.혼잡도군
					, B.CXNPS
					, 응답고객수
					, case when B.CXNPS is null then null 
						when B.CXNPS * 1.04 < A.NPS then 1
						when B.CXNPS * 0.96 <= A.NPS  and A.NPS <= B.CXNPS * 1.04 then 2
						when A.NPS < B.CXNPS * 0.96 then 3
						else null end CXNPS_Cmpr
					, NPS
			from(
					select 지역그룹관리코드, 지역본부합산조직코드
						, case when 70 <= avg(고객혼잡지수) then '01'
						when 60 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 70 then '02'
						when 40 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 60 then '03'
						when avg(고객혼잡지수) < 40  then '04'
						else '04'end 혼잡도군
						, 고객경험단계구분, sum(전체응답자수) as 응답고객수
						, (sum(추천고객수) - sum(비추천고객수) )/sum(전체응답자수) *100 as NPS
					from CXNPS_ACUM A
					where 설문응답년월 <= (select preMon from initData)
					group by 지역그룹관리코드, 지역본부합산조직코드, 고객경험단계구분
			)A
			left join All_CXNPS_PREMON B on A.지역그룹관리코드 = B.지역그룹관리코드 and A.혼잡도군 = B.혼잡도군 and A.고객경험단계구분 = B.고객경험단계구분
	), calNowMon as (-- (누적) CX_NPS(지역본부) 기준으로 혼잡도별 NPS에 따라 우수/양호/개선필요 구분
				select A.지역그룹관리코드
					, A.지역본부합산조직코드
					, A.고객경험단계구분
					, B.혼잡도군
					, B.CXNPS
					, 응답고객수
					, case when B.CXNPS is null then null 
						when B.CXNPS * 1.04 < A.NPS then 1
						when B.CXNPS * 0.96 <= A.NPS  and A.NPS <= B.CXNPS * 1.04 then 2
						when A.NPS < B.CXNPS * 0.96 then 3
						else null end CXNPS_Cmpr
					, NPS
				from(/*누적 NPS 계산*/
					select 지역그룹관리코드, 지역본부합산조직코드
						, case when 70 <= avg(고객혼잡지수) then '01'
						when 60 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 70 then '02'
						when 40 <= avg(고객혼잡지수) and avg(고객혼잡지수) < 60 then '03'
						when avg(고객혼잡지수) < 40  then '04'
						else '04'end 혼잡도군
						, 고객경험단계구분, sum(전체응답자수) as 응답고객수
						, (sum(추천고객수) - sum(비추천고객수) )/sum(전체응답자수) *100 as NPS
					from CXNPS_ACUM A
					where 설문응답년월 <= (select thisMon from initData)
					group by 지역그룹관리코드, 지역본부합산조직코드, 고객경험단계구분
			)A
			left join All_CXNPS_THISMON B on A.지역그룹관리코드 = B.지역그룹관리코드 and A.혼잡도군 = B.혼잡도군 and A.고객경험단계구분 = B.고객경험단계구분
			)
			select (select baseYmd from initData) as 기준년월일, A.지역그룹관리코드, 지역본부합산조직코드, A.고객경험단계구분, preMon_Cmpr, nowMon_Cmpr
				, 1month_Cmpr, 2month_Cmpr, 3month_Cmpr, 4month_Cmpr
				, 5month_Cmpr, 6month_Cmpr, 7month_Cmpr, 8month_Cmpr
				, 9month_Cmpr, 10month_Cmpr, 11month_Cmpr, 12month_Cmpr
			from(
			select A.지역그룹관리코드, 지역본부합산조직코드, A.고객경험단계구분, A.preMon_Cmpr, A.nowMon_Cmpr
					, sum(1month) as 1month_Cmpr
				, sum(2month) as 2month_Cmpr
				, sum(3month) as 3month_Cmpr
				, sum(4month) as 4month_Cmpr
				, sum(5month) as 5month_Cmpr
				, sum(6month) as 6month_Cmpr
				, sum(7month) as 7month_Cmpr
				, sum(8month) as 8month_Cmpr
				, sum(9month) as 9month_Cmpr
				, sum(10month) as 10month_Cmpr
				, sum(11month) as 11month_Cmpr
				, sum(12month) as 12month_Cmpr
			from(
					select  A.설문응답년월 as 기준년월, A.지역그룹관리코드 , A.지역본부합산조직코드, A.혼잡도군, A.고객경험단계구분
							, B.CXNPS_Cmpr as preMon_Cmpr
							, c.CXNPS_Cmpr as nowMon_Cmpr
							, case when substr(A.설문응답년월,5,2) = '01' then  A.CXNPS_Cmpr end 1month
							, case when substr(A.설문응답년월,5,2) = '02' then  A.CXNPS_Cmpr end 2month
							, case when substr(A.설문응답년월,5,2) = '03' then  A.CXNPS_Cmpr end 3month
							, case when substr(A.설문응답년월,5,2) = '04' then  A.CXNPS_Cmpr end 4month
							, case when substr(A.설문응답년월,5,2) = '05' then  A.CXNPS_Cmpr end 5month
							, case when substr(A.설문응답년월,5,2) = '06' then  A.CXNPS_Cmpr end 6month
							, case when substr(A.설문응답년월,5,2) = '07' then  A.CXNPS_Cmpr end 7month
							, case when substr(A.설문응답년월,5,2) = '08' then  A.CXNPS_Cmpr end 8month
							, case when substr(A.설문응답년월,5,2) = '09' then  A.CXNPS_Cmpr end 9month
							, case when substr(A.설문응답년월,5,2) = '10' then  A.CXNPS_Cmpr end 10month
							, case when substr(A.설문응답년월,5,2) = '11' then  A.CXNPS_Cmpr end 11month
							, case when substr(A.설문응답년월,5,2) = '12' then  A.CXNPS_Cmpr end 12month
						from cal A
						left join calPreMon B on A.지역본부합산조직코드 = B.지역본부합산조직코드 and A.고객경험단계구분 = b.고객경험단계구분
						left join calNowMon C on A.지역본부합산조직코드 = c.지역본부합산조직코드 and A.고객경험단계구분 = C.고객경험단계구분
						union all 
						select  A.설문응답년월 as 기준년월, A.지역그룹관리코드, A.지역본부합산조직코드, A.혼잡도군, '99' as 고객경험단계구분
							, B.응답고객수 as preMon_Cmpr
							, c.응답고객수 as nowMon_Cmpr
							, case when substr(A.설문응답년월,5,2) = '01' then  A.전체응답자수 end 1month
							, case when substr(A.설문응답년월,5,2) = '02' then  A.전체응답자수 end 2month
							, case when substr(A.설문응답년월,5,2) = '03' then  A.전체응답자수 end 3month
							, case when substr(A.설문응답년월,5,2) = '04' then  A.전체응답자수 end 4month
							, case when substr(A.설문응답년월,5,2) = '05' then  A.전체응답자수 end 5month
							, case when substr(A.설문응답년월,5,2) = '06' then  A.전체응답자수 end 6month
							, case when substr(A.설문응답년월,5,2) = '07' then  A.전체응답자수 end 7month
							, case when substr(A.설문응답년월,5,2) = '08' then  A.전체응답자수 end 8month
							, case when substr(A.설문응답년월,5,2) = '09' then  A.전체응답자수 end 9month
							, case when substr(A.설문응답년월,5,2) = '10' then  A.전체응답자수 end 10month
							, case when substr(A.설문응답년월,5,2) = '11' then  A.전체응답자수 end 11month
							, case when substr(A.설문응답년월,5,2) = '12' then  A.전체응답자수 end 12month
						from cal A
						left join calPreMon B on A.지역본부합산조직코드 = B.지역본부합산조직코드 and A.고객경험단계구분 = b.고객경험단계구분
						left join calNowMon C on A.지역본부합산조직코드 = c.지역본부합산조직코드 and A.고객경험단계구분 = C.고객경험단계구분
						where A.고객경험단계구분 = '00'
				)A 
				group by A.지역그룹관리코드, A.지역본부합산조직코드, A.고객경험단계구분
		)A 
		order by A.지역그룹관리코드, A.지역본부합산조직코드
)KK on MAIN.고객경험단계구분 = KK.고객경험단계구분
left join(
	SELECT DISTINCT A.기준년월일, 
		A.지역그룹관리코드,
		if(A.지역그룹관리코드 = '0000','본부직할', A.지역그룹관리구분명) AS 지역그룹관리구분명
	FROM inst1.tsccvci08 A 
	WHERE A.그룹회사코드='KB0'
	AND LENGTH(A.지역그룹관리구분명) > 0
	GROUP BY A.기준년월일, A.지역그룹관리코드
	ORDER BY A.지역그룹관리구분명 ASC
)GC on KK.기준년월일 = GC.기준년월일 and KK.지역그룹관리코드 = GC.지역그룹관리코드
left join(
	SELECT DISTINCT A.기준년월일
			, A.지역본부합산조직코드
			, if(A.지역본부합산조직코드 = '0000','본부직할지역본부', trim(A.지역본부합산조직명)) AS 지역본부합산조직명
			, (CASE WHEN INSTR(A.지역본부합산조직명,'(') > 0 THEN SUBSTR(A.지역본부합산조직명,1,INSTR(A.지역본부합산조직명,'(') - 1) ELSE A.지역본부합산조직명 END) AS hqNm2
	FROM inst1.tsccvci08 A 
	WHERE A.그룹회사코드='KB0'
	AND LENGTH(A.지역본부합산조직명) > 0
	GROUP BY A.기준년월일, A.지역본부합산조직코드
)HQ on KK.기준년월일 = HQ.기준년월일 and KK.지역본부합산조직코드 = HQ.지역본부합산조직코드
where 지역그룹관리구분명 = '{region_group_name}'
order by
	case
		when CAST(regexp_substr(substring_index(지역본부합산조직명, '(',1),'[0-9]+') as unsigned) is null then 99
		else CAST(regexp_substr(substring_index(지역본부합산조직명, '(',1),'[0-9]+') as unsigned)
	end,
	지역본부합산조직명,
	case MAIN.고객경험단계구분 when '00' then 1 when '03' then 2 else 99 end