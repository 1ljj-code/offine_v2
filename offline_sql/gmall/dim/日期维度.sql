DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING,
    `week_id`    STRING,
    `week_day`   STRING,
    `day`        STRING,
    `month`      STRING,
    `quarter`    STRING,
    `year`       STRING,
    `is_workday` STRING,
    `holiday_id` STRING
) COMMENT '日期维度表'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_date/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



DROP TABLE IF EXISTS tmp_dim_date_info;
CREATE EXTERNAL TABLE tmp_dim_date_info (
    `date_id`       STRING,
    `week_id`       STRING,
    `week_day`      STRING,
    `day`           STRING,
    `month`         STRING,
    `quarter`       STRING,
    `year`          STRING,
    `is_workday`    STRING,
    `holiday_id`    STRING
) COMMENT '时间维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info/';


insert overwrite table dim_date select * from tmp_dim_date_info;

select * from dim_date;