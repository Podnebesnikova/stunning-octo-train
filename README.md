with first_payments
  as 
(select user_id --1 шаг
, min (transaction_datetime :: date ) as first_payment_date
from skyeng_db.payments
where status_name = 'success'
group by user_id
)
,
-- 2
   all_dates 
   as
(select distinct class_start_datetime :: date as dt
        from skyeng_db.classes
     where  date_part ('year',class_start_datetime ) = 2016
)
--3
,
 all_dates_by_user
   as 
 (select user_id
 , dt
 from first_payments 
  join all_dates 
   on first_payments.first_payment_date <= all_dates.dt
   order by user_id, dt
   )
  
   --4
   ,
   payments_by_dates
     as 
  (select user_id
         , sum (classes)  as transaction_balance_change  
         ,transaction_datetime :: date as payment_date
      from skyeng_db.payments
       where status_name = 'success'
        and   date_part ('year',transaction_datetime ) = 2016  
      group by user_id 
             ,transaction_datetime::date
        order by user_id 
             ,payment_date
    ) 
    
   , payments_by_dates_cumsum
    as
    -- 5 
   (
   select a.user_id 
    , dt
     ,transaction_balance_change
    , sum (coalesce (transaction_balance_change,0)) over (partition by  a.user_id order by dt ) as transaction_balance_change_cs
    from all_dates_by_user a 
    left join payments_by_dates b
     on a.user_id = b.user_id
     and a.dt =  b.payment_date
     )
    
    -- 6 
    
    ,classes_by_dates 
    as
    (
    select 
     user_id
    ,class_start_datetime :: date as class_date
       ,count (id_class) * -1 as classes
    from skyeng_db.classes
    where class_type != 'trial'
          and class_status in ('success', 'failed_by_student')
         and date_part ('year',class_start_datetime ) = 2016
     
     group by user_id
    , class_date
     )
      -- 7 
        ,classes_by_dates_dates_cumsum 
        as 
     (select 
       a.user_id
       ,classes
       , dt
       , sum (coalesce (classes, 0)) over (partition by  a.user_id order by dt ) as classes_cs
     from all_dates_by_user a
       left join classes_by_dates b
        on a.user_id = b.user_id
        and a.dt = b.class_date
       )  
         -- 8 
        select  a. user_id
        , a. dt
        ,transaction_balance_change
        ,transaction_balance_change_cs
        ,classes 
        ,classes_cs
        ,classes_cs + transaction_balance_change_cs as balance
          from payments_by_dates_cumsum a
        join classes_by_dates_dates_cumsum b
         on a.user_id = b.user_id
         and a.dt = b.dt
         
         order by user_id
