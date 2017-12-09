use test20171208;


create table if not exists user_logs_p(
   	datetime string
	,user_id  int
	,order_id string
	,order_money int
	,user_em string
)
partitioned by (operate_datetime string)
stored as orc;
alter table user_logs_p drop partition(operate_datetime='${yyyymmdd}');
alter table user_logs_p add partition(operate_datetime='${yyyymmdd}');


insert overwrite table user_logs_p partition(operate_datetime='${yyyymmdd}')
select
	datetime
	,user_id
	,order_id
	,order_money
	,user_em
from user_logs
where datetime='${yyyymmdd}';