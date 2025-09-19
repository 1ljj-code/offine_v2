DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING,
    `activity_id`        STRING,
    `activity_name`      STRING,
    `activity_type_code` STRING,
    `activity_type_name` STRING,
    `activity_desc`      STRING,
    `start_time`         STRING,
    `end_time`           STRING,
    `create_time`        STRING,
    `condition_amount`   DECIMAL(16, 2),
    `condition_num`      BIGINT,
    `benefit_amount`     DECIMAL(16, 2,
    `benefit_discount`   DECIMAL(16, 2,
    `benefit_rule`       STRING,
    `benefit_level`      STRING
) COMMENT '活动维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');


insert overwrite table dim_activity_full partition(dt='20250916')
select
    rule.id,
    info.id,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
        end benefit_rule,
    benefit_level
from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from ods_activity_rule
        where dt=${bizdate}
    )rule
        left join
    (
        select
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        from ods_activity_info
        where dt=${bizdate}
    )info
    on rule.activity_id=info.id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where dt=${bizdate}
          and parent_code='31'
    )dic
    on rule.activity_type=dic.dic_code;