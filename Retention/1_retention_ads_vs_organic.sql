/*
В наших данных использования ленты новостей есть два типа юзеров: 
    те, кто пришел через платный трафик source = 'ads', 
    и те, кто пришел через органические каналы source = 'organic'.

Ваша задача — проанализировать и сравнить Retention этих двух групп пользователей. 
Решением этой задачи будет ответ на вопрос: 
отличается ли характер использования приложения у этих групп пользователей.
*/


--определяем все даты активности для юзеров
with users as (
    select user_id, source, toDate(time) as action_dt
    from simulator_20250320.feed_actions
    where toDate(time) >= today() - 30
    
),
--распределяем по когортам
users_min as (
    select user_id, source,
            min(action_dt) as start_dt,
            count(distinct user_id) over (partition by start_dt, source) as cohort_size
    from users
    group by user_id, source
),
--считаем retention по когортам
retention_by_cohorts as (
    select t1.source, t1.start_dt, t1.cohort_size as cohort_size, t2.action_dt, datediff('d', t1.start_dt, t2.action_dt) as days, 
            count(distinct t2.user_id) as retained_users,
            count(distinct t2.user_id) / t1.cohort_size as retention_rate
    from users_min t1
    join users t2 using (user_id)
    group by t1.source, t1.start_dt, t1.cohort_size, t2.action_dt, datediff('d', t1.start_dt, t2.action_dt)
)

--считаем средневзвешенный retention по каждому источнику трафика
select source, round(100 * avgWeighted(toFloat64(retention_rate), toFloat64(cohort_size))) as avgW_retention
from retention_by_cohorts
where days > 0 and days < 8 and start_dt between '2025-03-17' and '2025-03-23'
group by source

/*
Вывод: retention юзеров, которые пришли по органическим каналам, оказался выше, чем
    тех, кто пришел по платному трафику (22% против 16%)
*/

