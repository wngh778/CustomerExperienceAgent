with base as (
  select
    지역본부명,
    avg(고객혼잡지수) as avg_total,
    avg(case
          when 기준년월일 <= '{friday_b01w}' then 고객혼잡지수
        end) as avg_week,
    avg(case
          when 기준년월일 <= '{friday_b03w}' then 고객혼잡지수
        end) as avg_two_week
  from inst1.tsccvmgf3
  where 채널명 = '영업점'
    and 고객경험단계명 in ('해당무', '직원')
    and 지역영업그룹명 = '{region_group_name}'
    and 기준년월일 between '{yyyy0101}' and '{friday_b01w}'
  group by 지역본부명
),
base2 as (
select
  지역본부명,
  case
    when avg_total >= 70 then '01'
    when avg_total >= 60 and avg_total < 70 then '02'
    when avg_total >= 40 and avg_total < 60 then '03'
    else '04'
  end as 지역본부누적혼잡도군,
  case
    when avg_two_week >= 70 then '01'
    when avg_two_week >= 60 and avg_two_week < 70 then '02'
    when avg_two_week >= 40 and avg_two_week < 60 then '03'
    else '04'
  end as 지역본부격주혼잡도군,
  case
    when avg_week >= 70 then '01'
    when avg_week >= 60 and avg_week < 70 then '02'
    when avg_week >= 40 and avg_week < 60 then '03'
    else '04'
  end as 지역본부이번주혼잡도군
from base
)
select a.기준년월일, a.기준년월, a.채널명
  , case 
		when a.고객경험단계명='해당무' then '채널'
		else a.고객경험단계명
	end 고객경험단계명
  , a.추천점수, a.영향요인구분명, a.에피소드상세내용, a.지역영업그룹명, a.지역본부명, a.부점코드, a.고객혼잡지수, b.지역본부누적혼잡도군, b.지역본부격주혼잡도군, b.지역본부이번주혼잡도군
from inst1.tsccvmgf3 a
join base2 b on a.지역본부명 = b.지역본부명 
where a.채널명='영업점' 
and a.고객경험단계명 in ('해당무', '직원') 
and a.지역영업그룹명='{region_group_name}'
and a.기준년월일 between '{yyyy0101}' and '{friday_b01w}'