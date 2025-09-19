DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE
EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`                 STRING,
    `user_id`            STRING,
    `sku_id`             STRING,
    `date_id`            STRING,
    `create_time`        STRING,
    `sku_num`            BIGINT
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


set
hive.exec.dynamic.partition.mode=nonstrict;
insert
overwrite table dwd_trade_cart_add_inc partition (dt)
select id,
       user_id,
       sku_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       sku_num,
       date_format(create_time, 'yyyy-MM-dd')
from ods_cart_info
where dt = ${d};


insert
overwrite table dwd_trade_cart_add_inc partition (dt = '2022-06-09')
select id,
       user_id,
       sku_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd')                date_id,
       date_format(from_utc_timestamp(ts * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')       create_time,
       if(type = 'insert', sku_num, cast(sku_num as int) - cast(old['sku_num'] as int)) sku_num
from ods_cart_info
where dt = '2022-06-09'
  and (type = 'insert'
    or (type = 'update' and old['sku_num'] is not null and cast(data.sku_num as int) > cast(old['sku_num'] as int)));
