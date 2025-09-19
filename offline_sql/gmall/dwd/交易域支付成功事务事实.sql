DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE
EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING,
    `order_id`              STRING,
    `user_id`               STRING,
    `sku_id`                STRING,
    `province_id`           STRING,
    `activity_id`           STRING,
    `activity_rule_id`      STRING,
    `coupon_id`             STRING,
    `payment_type_code`     STRING,
    `payment_type_name`     STRING,
    `date_id`               STRING,
    `callback_time`         STRING,
    `sku_num`               BIGINT,
    `split_original_amount` DECIMAL(16, 2),
    `split_activity_amount` DECIMAL(16, 2),
    `split_coupon_amount`   DECIMAL(16, 2),
    `split_payment_amount`  DECIMAL(16, 2)
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



set
hive.exec.dynamic.partition.mode=nonstrict;
insert
overwrite table dwd_trade_pay_detail_suc_inc partition (dt)
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (select id,
             order_id,
             sku_id,
             sku_num,
             sku_num * order_price split_original_amount,
             split_total_amount,
             split_activity_amount,
             split_coupon_amount
      from ods_order_detail
      where dt = ${d}) od
         join
     (select user_id,
             order_id,
             payment_type,
             callback_time
      from ods_payment_info
      where dt = ${d}
        and payment_status = '1602') pi
     on od.order_id = pi.order_id
         left join
     (select id,
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
     on od.id = cou.order_detail_id
         left join
     (select dic_code,
             dic_name
      from ods_base_dic
      where dt = ${d}
        and parent_code = '11') pay_dic
     on pi.payment_type = pay_dic.dic_code;