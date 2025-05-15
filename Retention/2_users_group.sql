
/*
Необходимо на графике отобразить активную аудиторию ленты по неделям, для каждой недели выделим три типа пользователей.

Новые — первая активность в ленте была на этой неделе.
Старые — активность была и на этой, и на прошлой неделе.
Ушедшие — активность была на прошлой неделе, на этой не было.
*/

--считаем для каждого юзера неделю активности
with users as (
select distinct user_id, toMonday(toDate(time)) as week_dt
from simulator_20250320.feed_actions
),

--добавляем массив из недель, где юзер был активен
users_week as (
select t1.*, addWeeks(t1.week_dt, -1) as previous_week_dt,  addWeeks(t1.week_dt, 1) as next_week_dt, t2.weeks
from users t1
join (
    select user_id, groupArray(week_dt) as weeks
    from users
    group by user_id) t2 using user_id
)

--формируем итоговую таблицу со статусами и количеством
select week_dt, status, toInt64(count(distinct user_id)) as cnt_users
from (
    select t1.user_id, t1.week_dt, if(has(weeks, previous_week_dt) = 1, 'retained', 'new') as status
    from users_week t1
)
group by  week_dt, status

union all

select week_dt, status, count(distinct user_id) * (-1) as cnt_users
from (
select t1.user_id, t1.weeks, t1.next_week_dt as week_dt, 'gone' as status
from users_week t1
where has(weeks, next_week_dt) = 0 and t1.next_week_dt <= toMonday(toDate(today()))
)
group by  week_dt, status
