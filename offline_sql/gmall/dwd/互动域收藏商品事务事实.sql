drop table if exists dwd_interaction_favor_add_inc;
create external table if not exists dwd_interaction_favor_add_inc
(
    `id`          string,
    `user_id`     string,
    `sku_id`      string,
    `date_id`     string,
    `create_time` string
)comment '互动域收藏商品事务事实表'
partitioned by (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
tblproperties (
        'parquet.compress' = 'snappy',
        'external.table.purge' = 'true'
    );

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition (dt)
select 
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    date_format(create_time,'yyyy-MM-dd')
from ods_favor_info
where dt = ${d};

insert overwrite table dwd_interaction_favor_add_inc partition(dt='2022-06-09')
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time
from ods_favor_info
where dt=${d};
