DROP TABLE IF EXISTS dim_coupon_full;
CREATE
EXTERNAL TABLE dim_coupon_full
(
    `id`                STRING,
    `coupon_name`       STRING,
    `coupon_type_code`  STRING,
    `coupon_type_name`  STRING,
    `condition_amount`  DECIMAL(16, 2),
    `condition_num`     BIGINT,
    `activity_id`       STRING,
    `benefit_amount`    DECIMAL(16, 2),
    `benefit_discount`  DECIMAL(16, 2),
    `benefit_rule`      STRING,
    `create_time`       STRING,
    `range_type_code`   STRING,
    `range_type_name`   STRING,
    `limit_num`         BIGINT,
    `taken_count`       BIGINT,
    `start_time`        STRING,
    `end_time`          STRING,
    `operate_time`      STRING,
    `expire_time`       STRING
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_coupon/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert
overwrite table dim_coupon_full partition(dt='20250916')
select id,
       coupon_name,
       coupon_type,
       coupon_dic.dic_name,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       case coupon_type
           when '3201' then concat('满', condition_amount, '元减', benefit_amount, '元')
           when '3202' then concat('满', condition_num, '件打', benefit_discount, ' 折')
           when '3203' then concat('减', benefit_amount, '元')
           end benefit_rule,
       create_time,
       range_type,
       range_dic.dic_name,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
from (select id,
             coupon_name,
             coupon_type,
             condition_amount,
             condition_num,
             activity_id,
             benefit_amount,
             benefit_discount,
             create_time,
             range_type,
             limit_num,
             taken_count,
             start_time,
             end_time,
             operate_time,
             expire_time
      from ods_coupon_info
      where dt = ${bizdate}) ci
         left join
     (select dic_code,
             dic_name
      from ods_base_dic
      where dt = ${bizdate}
        and parent_code = '32') coupon_dic
     on ci.coupon_type = coupon_dic.dic_code
         left join
     (select dic_code,
             dic_name
      from ods_base_dic
      where dt = ${bizdate}
        and parent_code = '33') range_dic
     on ci.range_type = range_dic.dic_code;