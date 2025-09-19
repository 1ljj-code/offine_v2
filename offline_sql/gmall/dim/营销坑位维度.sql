DROP TABLE IF EXISTS dim_promotion_pos_full;
CREATE EXTERNAL TABLE dim_promotion_pos_full
(
    `id`               STRING,
    `pos_location`     STRING,
    `pos_type`         STRING,
    `promotion_type`   STRING,
    `create_time`      STRING,
    `operate_time`     STRING
) COMMENT '营销坑位维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_promotion_pos_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert overwrite table dim_promotion_pos_full partition(dt='20250916')
select
    `id`,
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods_promotion_pos
where dt=${bizdate};