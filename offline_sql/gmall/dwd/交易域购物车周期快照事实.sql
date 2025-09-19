DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE
EXTERNAL TABLE dwd_trade_cart_full
(
    `id`        STRING,
    `user_id`   STRING,
    `sku_id`    STRING,
    `sku_name`  STRING,
    `sku_num`   BIGINT
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');

insert
overwrite table dwd_trade_cart_full partition(dt='20250916')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info
where dt = ${d}
  and is_ordered = '0';