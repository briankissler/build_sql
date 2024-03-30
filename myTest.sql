


drop table myTest_card_status if exists;

Create Table myTest_card_status as Select Cm11, Cm13, cm15, dummy, Revolve from card_status as Cm where as_of_date=’12/01/2024’;



drop table myTest_ardw_account if exists;

Create Table myTest_ardw_account as Select Cm11, Cm13, cm15, Testdummy, account_revolve from ardw_account as ardw where rpt_date=’12/01/2024’;



drop table myTest_card_statusBasePop if exists;

Create Table myTest_card_statusBasePop as select * from (
    from (
    Select *, rand() as rw
    From myTest_card_status) x order by rw desc limit 10000);

select Cm.Cm11 as Cm11_Cm, 
ardw.Cm11 as Cm11_ardw, 
Cm.Cm13 as Cm13_Cm, 
ardw.Cm13 as Cm13_ardw, 
Cm.cm15 as cm15_Cm, 
ardw.cm15 as cm15_ardw, 
Cm.dummy as dummy_Cm, 
ardw.Testdummy as Testdummy_ardw, 
Cm.Revolve as Revolve_Cm
ardw.account_revolve as account_revolve_ardw
case When coalesce( Cm.Cm11,'') = coalesce( ardw.Cm11,'') then 0 else 1 end as Cm11, 
case When coalesce( Cm.Cm13,'') = coalesce( ardw.Cm13,'') then 0 else 1 end as Cm13, 
case When coalesce( Cm.cm15,'') = coalesce( ardw.cm15,'') then 0 else 1 end as cm15, 
case When coalesce( Cm.dummy,'') = coalesce( ardw.Testdummy,'') then 0 else 1 end as dummy, 
case When coalesce( Cm.Revolve,'') = coalesce( ardw.account_revolve,'') then 0 else 1 end as Revolve
 from myTest_ardw_accountBasePop
 join myTest_Cm\, on (Cm.cm15=ardw.cm15 )
