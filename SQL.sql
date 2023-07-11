with first_payments as 
(
    select     user_id
            ,  date_trunc('day', min(transaction_datetime)) as first_payment_date
        from skyeng_db.payments
        where status_name = 'success'
        group by user_id
),
----------------------------
all_dates as    
(  
        select  distinct date(class_start_datetime) as dt
        from skyeng_db.classes
        where class_start_datetime between '2016-01-01' and '2017-01-01'
),
----------------------------
all_dates_by_user as 
(
    select      user_id
            ,   dt
    from first_payments a
    join all_dates b on
        dt >= first_payment_date
),
----------------------------
payments_by_dates as
(
        select      user_id
                ,   date_trunc('day', transaction_datetime) as payment_date
                ,   sum(classes) as transaction_balance_change
        from skyeng_db.payments
        where status_name = 'success'
        group by user_id, payment_date
),
----------------------------
payments_by_dates_cumsum as
(
    select      c.user_id
            ,   dt
            ,   transaction_balance_change
            ,   sum(transaction_balance_change) over (partition by c.user_id order by dt) as transaction_balance_change_cs
    from all_dates_by_user c
    left join payments_by_dates d on
    c.user_id = d.user_id and c.dt = d.payment_date 
),
----------------------------
classes_by_dates as 
(
    select      user_id
            ,   date_trunc('day', class_start_datetime) as class_date
            ,   count(id_class)*(-1) as classes
    from skyeng_db.classes
    where class_status in ('success', 'failed_by_student')
    group by user_id, class_date
),
----------------------------
classes_by_dates_dates_cumsum as
(
    select      e.user_id
            ,   dt
            ,   classes
            ,   sum(coalesce ("classes", 0)) over (partition by e.user_id order by dt) as classes_cs
    from all_dates_by_user e
    left join classes_by_dates f on
    e.dt = f.class_date and e.user_id = f.user_id
),
----------------------------
balances as
(
    select      g.user_id
            ,   g.dt
            ,   transaction_balance_change
            ,   transaction_balance_change_cs
            ,   classes
            ,   classes_cs
            ,   classes_cs + transaction_balance_change_cs as balanse
    from classes_by_dates_dates_cumsum g
    join payments_by_dates_cumsum h on
    g.user_id = h.user_id and g.dt = h.dt
)
----------------------------
-- select *             --Задание 1
-- from balances
-- order by user_id, dt
-- limit 1000
----------------------------
select           --Задание 2
            dt
        ,   sum(transaction_balance_change)::integer as sum_transaction_balance_change
        ,   sum(transaction_balance_change_cs)::integer as sum_transaction_balance_change_cs
        ,   sum(classes)::integer as sum_classes
        ,   sum(classes_cs)::integer as sum_classes_cs
        ,   sum(balanse)::integer as sum_balances
from balances
group by dt
order by dt