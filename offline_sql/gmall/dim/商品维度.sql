DROP TABLE IF EXISTS dim_sku_full;
CREATE
EXTERNAL TABLE dim_sku_full
(
    `id`                   STRING,
    `price`                DECIMAL(16, 2),
    `sku_name`             STRING,
    `sku_desc`             STRING,
    `weight`               DECIMAL(16, 2),
    `is_sale`              BOOLEAN,
    `spu_id`               STRING,
    `spu_name`             STRING,
    `category3_id`         STRING,
    `category3_name`       STRING,
    `category2_id`         STRING,
    `category2_name`       STRING,
    `category1_id`         STRING,
    `category1_name`       STRING,
    `tm_id`                STRING,
    `tm_name`              STRING,
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,
            value_id :STRING,
            attr_name :STRING,
            value_name:STRING
            >
       >,
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,
        sale_attr_value_id :STRING,
        sale_attr_name :STRING,
        sale_attr_value_name:STRING>>,
    `create_time`          STRING
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');



with sku as
         (select id,
                 price,
                 sku_name,
                 sku_desc,
                 weight,
                 is_sale,
                 spu_id,
                 category3_id,
                 tm_id,
                 create_time
          from ods_sku_info
          where dt = ${bizdate}),
     spu as
         (select id,
                 spu_name
          from ods_spu_info
          where dt = ${bizdate}),
     c3 as
         (select id,
                 name,
                 category2_id
          from ods_base_category3
          where dt = ${bizdate}),
     c2 as
         (select id,
                 name,
                 category1_id
          from ods_base_category2
          where dt = ${bizdate}),
     c1 as
         (select id,
                 name
          from ods_base_category1
          where dt = ${bizdate}),
     tm as
         (select id,
                 tm_name
          from ods_base_trademark
          where dt = ${bizdate}),
     attr as
         (select sku_id,
                 collect_set(named_struct('attr_id', attr_id, 'value_id', value_id, 'attr_name', attr_name,
                                          'value_name', value_name)) attrs
          from ods_sku_attr_value
          where dt = ${bizdate}
          group by sku_id),
     sale_attr as
         (select sku_id,
                 collect_set(named_struct('sale_attr_id', sale_attr_id, 'sale_attr_value_id', sale_attr_value_id,
                                          'sale_attr_name', sale_attr_name, 'sale_attr_value_name',
                                          sale_attr_value_name)) sale_attrs
          from ods_sku_sale_attr_value
          where dt = ${bizdate}
          group by sku_id)


insert
overwrite table dim_sku_full partition(dt= '20250916')
select sku.id,
       sku.price,
       sku.sku_name,
       sku.sku_desc,
       sku.weight,
       sku.is_sale,
       sku.spu_id,
       spu.spu_name,
       sku.category3_id,
       c3.name,
       c3.category2_id,
       c2.name,
       c2.category1_id,
       c1.name,
       sku.tm_id,
       tm.tm_name,
       attr.attrs,
       sale_attr.sale_attrs,
       sku.create_time
from sku
         left join spu on sku.spu_id = spu.id
         left join c3 on sku.category3_id = c3.id
         left join c2 on c3.category2_id = c2.id
         left join c1 on c2.category1_id = c1.id
         left join tm on sku.tm_id = tm.id
         left join attr on sku.id = attr.sku_id
         left join sale_attr on sku.id = sale_attr.sku_id;

