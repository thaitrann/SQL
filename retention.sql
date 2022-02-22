--- retention rate by day
select user_data.activity_date,
count(distinct user_data.user_id) as active_users,
count(distinct future_activity.user_id) as retained_users,
cast(count(distinct future_activity.user_id) as float)/cast(count(distinct user_data.user_id) as float) as retention
from user_data
left join user_data as future_activity on
user_data.user_id = future_activity.user_id
and  user_data.activity_date = dateadd(day, -1, future_activity.activity_date)
group by user_data.activity_date
order by user_data.activity_date

create table new_user_by_day(
	first_day date,
	new_users int
)
-- new user by day
with user_first_day (user_id, first_day) 
as
(
	select 
		user_id, 
		min(activity_date) as first_day
	from user_data
	group by user_id
)
--insert into new_user_by_day
select 
	first_day,
	count(user_id) as new_users
from user_first_day
group by first_day
order by first_day;

--retain_user_by_day
with user_retention_day (user_id, retention_day) as
(
	select user_id, activity_date as retention_day
	from user_data
	group by user_id, activity_date
), 
user_first_day (user_id, first_day) 
as
(
	select 
		user_id, 
		min(activity_date) as first_day
	from user_data
	group by user_id
)
--insert into retain_user_by_day
select 
	user_first_day.first_day,
	user_retention_day.retention_day,
	count(user_retention_day.user_id) as retained_user
from user_retention_day
left join user_first_day on user_retention_day.user_id = user_first_day.user_id
group by
	user_first_day.first_day,
	user_retention_day.retention_day
order by 1,2 

create table retain_user_by_day(
	first_day date,
	retention_day date,
	retained_user int
)

insert into retention_rate
-- retention_rate
select 
	retain_user_by_day.first_day,
	retain_user_by_day.retention_day,
	new_user_by_day.new_users,
	retain_user_by_day.retained_user,
	cast(retain_user_by_day.retained_user as float)/cast(new_user_by_day.new_users as float) as retention_rate
from retain_user_by_day
left join new_user_by_day 
on retain_user_by_day.first_day = new_user_by_day.first_day
order by 1,2 

create table retention_rate(
	first_day date,
	retention_day date,
	new_users int,
	retained_user int,
	retention_rate float
)

