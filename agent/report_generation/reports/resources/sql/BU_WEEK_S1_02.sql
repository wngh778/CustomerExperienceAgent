with 발송 as ( select sum(설문발송건수) as 발송건수, sum(응답접속수) as 응답접속건수
from inst1.tsccvsv71
where 설문ID in (select distinct 설문ID from inst1.tsccvsv73 where 그룹회사코드='KB0' and 설문응답종료년월일 >= '{yyyymmdd_cx_manage}' and 설문조사방식구분='02' and 설문조사종류구분='03')
and 기준년월일 between '{yyyymmdd_cx_manage}' and '{biz_endday_b01w}'
),
응답접속 as(select sum(응답접속) as  응답접속
		from(
			select 설문ID
				, 1 as 응답접속
			from inst1.tsccvsv23
			where 설문응답시작일시 between '{yyyymmdd_cx_manage}' and '{biz_endday_b01w}'
			and 설문ID in (select 설문ID
							from inst1.tsccvsv11
							where 설문조사방식구분 = '02'
							and 설문조사종류구분 = '03'
							and 설문진행상태구분 = '06'
							)
			)z ),
응답 as (
select count(설문참여대상자고유ID) as 응답완료
from inst1.tsccvmgb7 t
where 관리지표채널구분 in ('01','02','03')
and 기준년월일 between '{yyyymmdd_cx_manage}' and '{biz_endday_b01w}'
)
select 발송건수 AS 총발송건수, 응답접속 AS 총응답접속수, 응답완료 AS 총응답완료수, round(응답완료/발송건수*100,2) as 응답률
from 발송, 응답접속, 응답;
