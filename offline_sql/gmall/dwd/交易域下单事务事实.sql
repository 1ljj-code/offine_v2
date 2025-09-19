DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE
EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                    STRING,
    `order_id`              STRING,
    `user_id`               STRING,
    `sku_id`                STRING,
    `province_id`           STRING,
    `activity_id`           STRING,
    `activity_rule_id`      STRING,
    `coupon_id`             STRING,
    `date_id`               STRING,
    `create_time`           STRING,
    `sku_num`               BIGINT,
    `split_original_amount` DECIMAL(16, 2),
    `split_activity_amount` DECIMAL(16, 2),
    `split_coupon_amount`   DECIMAL(16, 2),
    `split_total_amount`    DECIMAL(16, 2)
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');




set
hive.exec.dynamic.partition.mode=nonstrict;
insert
overwrite table dwd_trade_order_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (select id,
             order_id,
             sku_id,
             create_time,
             sku_num,
             sku_num * order_price split_original_amount,
             split_total_amount,
             split_activity_amount,
             split_coupon_amount
      from ods_order_detail
      where dt = ${d}) od
         left join
     (select id,
             user_id,
             province_id
      from ods_order_info
      where dt = ${d}) oi
     on od.order_id = oi.id
         left join
     (select order_detail_id,
             activity_id,
             activity_rule_id
      from ods_order_detail_activity
      where dt = ${d}) act
     on od.id = act.order_detail_id
         left join
     (select order_detail_id,
             coupon_id
      from ods_order_detail_coupon
      where dt = ${d}) cou
     on od.id = cou.order_detail_id;