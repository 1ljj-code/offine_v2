DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full
(
    `id`            STRING,
    `province_name` STRING,
    `area_code`     STRING,
    `iso_code`      STRING,
    `iso_3166_2`    STRING,
    `region_id`     STRING,
    `region_name`   STRING
) COMMENT '地区维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



insert overwrite table dim_province_full partition(dt='20250916')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
    (
        select
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        from ods_base_province
        where dt=${bizdate}
    )province
        left join
    (
        select
            id,
            region_name
        from ods_base_region
        where dt=${bizdate}
    )region
    on province.region_id=region.id;