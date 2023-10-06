with first_payments as
    (
    select user_id
         , date_trunc ('day', min (transaction_datetime)) as first_payment_date
    from skyeng_db.payments
    where status_name = 'success'
    group by user_id
    ),
all_dates  as
    (
    select distinct date_trunc('day', class_start_datetime) as dt
    from skyeng_db.classes
    where class_start_datetime between '2016-01-01 00:00' and '2016-12-31 23:59'
    ),
all_dates_by_user as
    (
    select  first_payments.user_id
          , all_dates.dt
    from all_dates
        join first_payments
            on all_dates.dt >= first_payments.first_payment_date
    ),
payments_by_dates as
    (
    select user_id
         , date_trunc('day', transaction_datetime) as payment_date
         , sum (classes) as transaction_balance_change 
    from skyeng_db.payments
    where status_name = 'success'
    group by user_id
        , payment_date
    ),
payments_by_dates_cumsum as
    (
    select all_dates_by_user.user_id
         , all_dates_by_user.dt
         , transaction_balance_change
         , sum(coalesce (transaction_balance_change, 0)) over (partition by all_dates_by_user.user_id order by dt) as transaction_balance_change_cs 
    from all_dates_by_user 
        left join payments_by_dates 
            on all_dates_by_user.user_id=payments_by_dates.user_id
            and all_dates_by_user.dt=payments_by_dates.payment_date
    ),
classes_by_dates as
    (
    select  user_id
        , date_trunc('day', class_start_datetime) as class_date
        , -count (* ) as classes 
    from skyeng_db.classes
    where class_type != 'trial'
     and class_status in ('success', 'failed_by_student')
    group by user_id , class_date
    ),
classes_by_dates_dates_cumsum as
    (
    select all_dates_by_user.user_id
        , all_dates_by_user.dt
        , classes
        , sum(coalesce (classes, 0)) over (partition by all_dates_by_user.user_id order by dt) as classes_cs 
    from all_dates_by_user 
        left join classes_by_dates 
            on all_dates_by_user.user_id=classes_by_dates.user_id
            and all_dates_by_user.dt=classes_by_dates.class_date
    ),
balances  as
    (
    select payments_by_dates_cumsum.user_id
        , payments_by_dates_cumsum.dt
        , transaction_balance_change
        , transaction_balance_change_cs
        , classes
        , classes_cs
        , (transaction_balance_change_cs+classes_cs ) as balance 
    from payments_by_dates_cumsum 
        join classes_by_dates_dates_cumsum 
            on payments_by_dates_cumsum.user_id=classes_by_dates_dates_cumsum.user_id
            and payments_by_dates_cumsum.dt=classes_by_dates_dates_cumsum.dt
    )
select user_id
    , dt
    , balance
    from balances
order by user_id
    , dt
limit 1000

-----------
-- select dt
--     , sum (transaction_balance_change) as transaction_balance_change_sum
--     , sum (transaction_balance_change_cs) as transaction_balance_change_cs_sum
--     , sum (classes) as classes_sum
--     , sum (classes_cs) as classes_cs_sum
--     , sum (balance) as balance_sum
-- from balances
-- group by dt
-- order by dt asc



    
    
