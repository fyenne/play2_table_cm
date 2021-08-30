# modify created table of receipt_header_df

from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import * 
import numpy as np

import subprocess
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
from pyspark.sql.types import StructType, LongType, StringType, StructField, DoubleType
"""
receipt_header
"""
# -- hive -e "CREATE TABLE IF NOT EXISTS ods_cn_bose.receipt_header_df AS SELECT *, cast(null as string) as src_inc_day FROM ods_cn_bose.receipt_header where inc_day = '20210826'"


names = [
'ods_cn_bose'
# ,'ods_cn_apple_sz'
# ,'ods_cn_apple_sh'
# ,'ods_cn_costacoffee'
# ,'ods_cn_diadora'
# ,'ods_cn_ferrero'
# ,'ods_cn_fuji'
# ,'ods_cn_hd'
# ,'ods_cn_hp_ljb'
# ,'ods_cn_hpi'
# ,'ods_cn_hualiancosta'
# ,'ods_cn_jiq'
# ,'ods_cn_kone'
# ,'ods_cn_michelin'
# ,'ods_cn_razer'
# ,'ods_cn_squibb'
# ,'ods_cn_vzug'
# ,'ods_cn_zebra'
# ,'ods_dbo'
# ,'ods_hk_abbott'
# ,'ods_hk_revlon'
# ,'ods_hk_fredperry'
] 

"""
三个部分, 第一部分创建表.
"""

rh1 = ['CREATE EXTERNAL TABLE IF NOT EXISTS ' + i +
"""
.receipt_header_df (
`internal_receipt_num` double COMMENT '4',
`warehouse` string COMMENT '4',
`company` string COMMENT '4',
`receipt_id` string COMMENT '4',
`receipt_id_type` string COMMENT '4',
`receipt_type` string COMMENT '4',
`receipt_date` string COMMENT '4',
`close_date` string COMMENT '4',
`source_id` string COMMENT '4',
`source_name` string COMMENT '4',
`source_address1` string COMMENT '4',
`source_address2` string COMMENT '4',
`source_address3` string COMMENT '4',
`source_city` string COMMENT '4',
`source_state` string COMMENT '4',
`source_postal_code` string COMMENT '4',
`source_country` string COMMENT '4',
`source_attention_to` string COMMENT '4',
`source_phone_num` string COMMENT '4',
`source_fax_num` string COMMENT '4',
`source_email_address` string COMMENT '4',
`priority` double COMMENT '4',
`carrier` string COMMENT '4',
`carrier_service` string COMMENT '4',
`erp_order_num` string COMMENT '4',
`erp_order_type` string COMMENT '4',
`bol_num_alpha` string COMMENT '4',
`license_plate_id` string COMMENT '4',
`packing_list_id` string COMMENT '4',
`pro_num_alpha` string COMMENT '4',
`trailer_id` string COMMENT '4',
`seal_id` string COMMENT '4',
`total_containers` double COMMENT '4',
`total_lines` double COMMENT '4',
`total_qty` double COMMENT '4',
`quantity_um` string COMMENT '4',
`total_weight` double COMMENT '4',
`weight_um` string COMMENT '4',
`total_volume` double COMMENT '4',
`volume_um` string COMMENT '4',
`total_value` double COMMENT '4',
`leading_sts` double COMMENT '4',
`leading_sts_date` string COMMENT '4',
`leading_sts_failed` string COMMENT '4',
`trailing_sts` double COMMENT '4',
`trailing_sts_date` string COMMENT '4',
`trailing_sts_failed` string COMMENT '4',
`user_def1` string COMMENT '4',
`user_def2` string COMMENT '4',
`user_def3` string COMMENT '4',
`user_def4` string COMMENT '4',
`user_def5` string COMMENT '4',
`user_def6` string COMMENT '4',
`user_def7` double COMMENT '4',
`user_def8` double COMMENT '4',
`user_stamp` string COMMENT '4',
`process_stamp` string COMMENT '4',
`date_time_stamp` string COMMENT '4',
`manually_entered` string COMMENT '4',
`ship_from` string COMMENT '4',
`ship_from_address1` string COMMENT '4',
`ship_from_address2` string COMMENT '4',
`ship_from_address3` string COMMENT '4',
`ship_from_city` string COMMENT '4',
`ship_from_state` string COMMENT '4',
`ship_from_country` string COMMENT '4',
`ship_from_postal_code` string COMMENT '4',
`ship_from_name` string COMMENT '4',
`ship_from_attention_to` string COMMENT '4',
`ship_from_email_address` string COMMENT '4',
`ship_from_phone_num` string COMMENT '4',
`ship_from_fax_num` string COMMENT '4',
`scheduled_date_time` string COMMENT '4',
`arrived_date_time` string COMMENT '4',
`start_unitize_date_time` string COMMENT '4',
`end_unitize_date_time` string COMMENT '4',
`interface_record_id` string COMMENT '4',
`creation_process_stamp` string COMMENT '4',
`creation_date_time_stamp` string COMMENT '4',
`trailer_yard_status_id` double COMMENT '4',
`upload_interface_batch` string COMMENT '4',
`in_pre_checkin_ctr_creation` string COMMENT '4',
`src_inc_day` string comment '数据来源的inc_day') 
COMMENT 'RECEIPT_HEADER' 
PARTITIONED BY (`inc_day` string COMMENT '增量日期') 
stored as parquet 
LOCATION 'hdfs://dsc/hive/warehouse/dsc/ods/
"""
+ i + "/receipt_detail_df'"
for i in names]
# hdfs://dsc/hive/warehouse/dsc/ods/ods_cn_apple_sh/receipt_header


rh2 = [i.replace('\n', '') for i in rh1]

[spark.sql(i) for i in rh2]



rd1 = ['CREATE EXTERNAL TABLE IF NOT EXISTS ' + i +
    """.receipt_detail_df (
    `internal_receipt_line_num` double COMMENT 'S',
    `locating_rule` string COMMENT 'D',
    `warehouse` string COMMENT 'S',
    `internal_receipt_num` double COMMENT 'G',
    `receipt_id` string COMMENT 'D',
    `item` string COMMENT 'G',
    `item_desc` string COMMENT 'F',
    `item_class` string COMMENT 'H',
    `lot` string COMMENT 'G',
    `company` string COMMENT 'F',
    `total_qty` double COMMENT 'G',
    `quantity_um` string COMMENT 'G',
    `open_qty` double COMMENT 'GF',
    `erp_order_num` string COMMENT 'F',
    `erp_order_type` string COMMENT 'G',
    `erp_order_line_num` double COMMENT 'F',
    `expiration_date_time` string COMMENT 'G',
    `manufactured_date_time` string COMMENT 'G',
    `item_weight` double COMMENT 'F',
    `weight_um` string COMMENT 'G',
    `item_length` double COMMENT 'F',
    `item_width` double COMMENT 'G',
    `item_height` double COMMENT 'F',
    `item_dimension_um` string COMMENT 'G',
    `volume_um` string COMMENT 'F',
    `item_division` string COMMENT 'G',
    `item_department` string COMMENT 'FG',
    `value` double COMMENT 'H',
    `item_size` string COMMENT 'F',
    `item_color` string COMMENT 'G',
    `item_style` string COMMENT 'F',
    `launch_num` double COMMENT 'G',
    `put_loc` string COMMENT 'F',
    `user_def1` string COMMENT 'G',
    `user_def2` string COMMENT 'F',
    `user_def3` string COMMENT 'G',
    `user_def4` string COMMENT 'F',
    `user_def5` string COMMENT 'G',
    `user_def6` string COMMENT 'F',
    `user_def7` double COMMENT 'H',
    `user_def8` double COMMENT 'F',
    `user_stamp` string COMMENT 'D',
    `process_stamp` string COMMENT 'G',
    `date_time_stamp` string COMMENT 'S',
    `put_zone` string COMMENT 'GG',
    `item_list_price` double COMMENT 'F',
    `item_net_price` double COMMENT 'F',
    `put_list_num` double COMMENT 'H',
    `manually_entered` string COMMENT 'F',
    `lot_controlled` string COMMENT 'G',
    `serial_num_reqd` string COMMENT 'F',
    `catch_weight_reqd` string COMMENT 'D',
    `source_id` string COMMENT 'GD',
    `hazardous_code` string COMMENT 'H',
    `total_weight` double COMMENT 'F',
    `status_flow_name` string COMMENT 'D',
    `expected_inv_sts` string COMMENT 'F',
    `ship_from` string COMMENT 'F',
    `conversion_qty` double COMMENT 'FD',
    `conversion_um` string COMMENT 'G',
    `priority` double COMMENT 'F',
    `customer_order_num` string COMMENT 'D',
    `receipt_date` string COMMENT 'G',
    `conversion_height` double COMMENT 'F',
    `conversion_width` double COMMENT 'F',
    `conversion_length` double COMMENT 'D',
    `conversion_weight` double COMMENT 'F',
    `original_total_quantity` double COMMENT 'F',
    `interface_record_id` string COMMENT 'D',
    `item_category1` string COMMENT 'F',
    `item_category2` string COMMENT 'DF',
    `item_category3` string COMMENT 'F',
    `item_category4` string COMMENT 'D',
    `item_category5` string COMMENT 'DF',
    `item_category6` string COMMENT 'A',
    `item_category7` string COMMENT 'FD',
    `item_category8` string COMMENT 'D',
    `item_category9` string COMMENT 'F',
    `item_category10` string COMMENT 'F',
    `inventory_sts` string COMMENT 'F',
    `purchase_order_detail_id` double COMMENT 'G',
    `purchase_order_id` string COMMENT 'F',
    `purchase_order_line_number` double COMMENT 'HF',
    `src_inc_day` string comment '数据来源的inc_day'
    ) COMMENT 'receipt-detail' 
    PARTITIONED BY (`inc_day` string COMMENT '增量日期') 
    STORED AS Parquet 
    LOCATION 'hdfs://dsc/hive/warehouse/dsc/ods/
"""
+ i + "/receipt_detail_df'"
for i in names]

rd2 = [i.replace('\n', '') for i in rd1]
[spark.sql(i) for i in rd2]




sh1 = ['CREATE EXTERNAL TABLE IF NOT EXISTS ' + i + 
""".shipment_header_df (
    `internal_shipment_num` double COMMENT 'o',
    `warehouse` string COMMENT 'uy',
    `shipping_load_num` double COMMENT 'i',
    `shipment_id` string COMMENT 'o',
    `launch_step` string COMMENT 'ioi',
    `trailing_sts` double COMMENT 'o',
    `trailing_sts_date` string COMMENT 'i',
    `trailing_sts_failed` string COMMENT 'o',
    `order_type` string COMMENT 'o',
    `consolidated` string COMMENT 'i',
    `leading_sts` double COMMENT 'o',
    `leading_sts_date` string COMMENT 'i',
    `carrier` string COMMENT 'o',
    `leading_sts_failed` string COMMENT 'i',
    `company` string COMMENT 'o',
    `carrier_service` string COMMENT 'i',
    `carrier_group` string COMMENT 'o',
    `carrier_type` string COMMENT 'i',
    `freight_terms` string COMMENT 'o',
    `liability_terms` string COMMENT 'i',
    `customer` string COMMENT 'i',
    `ship_to` string COMMENT 'o',
    `route` string COMMENT 'i',
    `stop` string COMMENT 'o',
    `ship_to_address1` string COMMENT 'o',
    `customer_name` string COMMENT 'i',
    `ship_to_address2` string COMMENT 'o',
    `ship_to_address3` string COMMENT 'i',
    `ship_to_city` string COMMENT 'o',
    `ship_to_state` string COMMENT 'i',
    `ship_to_country` string COMMENT 'o',
    `ship_to_postal_code` string COMMENT 'o',
    `ship_to_phone_num` string COMMENT 'o',
    `ship_to_fax_num` string COMMENT 'o',
    `customer_address1` string COMMENT 'i',
    `launch_num` double COMMENT 'o',
    `weight_um` string COMMENT 'o',
    `volume_um` string COMMENT 'i',
    `routing_code` string COMMENT 'o',
    `single_item_cartons` string COMMENT 'o',
    `user_def1` string COMMENT 'o',
    `user_def2` string COMMENT 'of',
    `priority` double COMMENT 'f',
    `user_def3` string COMMENT 'u',
    `user_def4` string COMMENT 'yt',
    `user_def5` string COMMENT 'u',
    `user_def6` string COMMENT 'o',
    `user_def7` double COMMENT 'i',
    `manually_entered` string COMMENT 'o',
    `user_def8` double COMMENT 'o',
    `user_stamp` string COMMENT 'i',
    `process_stamp` string COMMENT 'o',
    `date_time_stamp` string COMMENT 'i',
    `bol_num_alpha` string COMMENT 'o',
    `pro_num_alpha` string COMMENT 'o',
    `customer_address3` string COMMENT 'o',
    `customer_address2` string COMMENT 'oi',
    `customer_attention_to` string COMMENT 'i',
    `customer_city` string COMMENT 'o',
    `customer_state` string COMMENT 'o',
    `customer_country` string COMMENT 'oi',
    `customer_postal_code` string COMMENT 'i',
    `customer_phone_num` string COMMENT 'i',
    `customer_fax_num` string COMMENT 'o',
    `customer_email_address` string COMMENT 'i',
    `ship_to_name` string COMMENT 'o',
    `ship_to_attention_to` string COMMENT 'i',
    `ship_to_email_address` string COMMENT 'o',
    `requested_delivery_date` string COMMENT 'o',
    `requested_delivery_type` string COMMENT 'i',
    `scheduled_ship_date` string COMMENT 'o',
    `planned_ship_date` string COMMENT 'i',
    `actual_ship_date_time` string COMMENT 'o',
    `planned_delivery_date_time` string COMMENT 'io',
    `actual_delivery_date_time` string COMMENT 'o',
    `quantity_um` string COMMENT 'i',
    `total_freight_charge` double COMMENT 'o',
    `base_freight_charge` double COMMENT 'o',
    `freight_discount` double COMMENT 'i',
    `accessorial_charge` double COMMENT 'o',
    `consolidation_allowed` string COMMENT 'j',
    `intermediate_consignee` string COMMENT 'hj',
    `intermediate_name` string COMMENT 'h',
    `intermediate_address1` string COMMENT 'g',
    `intermediate_address2` string COMMENT 'gj',
    `intermediate_address3` string COMMENT 'h',
    `intermediate_attention_to` string COMMENT 'h',
    `intermediate_city` string COMMENT 'h',
    `intermediate_state` string COMMENT 'hg',
    `intermediate_country` string COMMENT 'g',
    `intermediate_postal_code` string COMMENT 'j',
    `intermediate_phone_num` string COMMENT 'g',
    `intermediate_fax_num` string COMMENT 'gh',
    `intermediate_email_address` string COMMENT 'h',
    `freight_bill_to` string COMMENT 'hg',
    `freight_bill_to_name` string COMMENT 'hg',
    `freight_bill_to_address1` string COMMENT 'gh',
    `freight_bill_to_address2` string COMMENT 'j',
    `freight_bill_to_address3` string COMMENT 'g',
    `freight_bill_to_attention_to` string COMMENT 'gh',
    `freight_bill_to_city` string COMMENT 'h',
    `freight_bill_to_state` string COMMENT 'jg',
    `freight_bill_to_country` string COMMENT 'g',
    `freight_bill_to_postal_code` string COMMENT 'f',
    `freight_bill_to_phone_num` string COMMENT 'm',
    `freight_bill_to_fax_num` string COMMENT 'n',
    `freight_bill_to_email_address` string COMMENT 'fg',
    `erp_order` string COMMENT 'fg',
    `internal_order_num` double COMMENT 'h',
    `export_tax_id` string COMMENT 'gf',
    `parties` string COMMENT 'f',
    `loading_pier` string COMMENT 'h',
    `transportation_mode` string COMMENT 'h',
    `export_port` string COMMENT 'f',
    `unloading_port` string COMMENT 'g',
    `containerized` string COMMENT 'gf',
    `ftz` string COMMENT 'f',
    `validated_license` string COMMENT 'g',
    `license_exp_date` string COMMENT 'b',
    `eccn` string COMMENT 'v',
    `authorized_empl_name` string COMMENT 'h',
    `authorized_empl_title` string COMMENT 'f',
    `upload_interface_batch` string COMMENT 'hf',
    `customer_residential_flag` string COMMENT 'g',
    `shipto_residential_flag` string COMMENT 'j',
    `rejection_note` string COMMENT 'gh',
    `interface_record_id` string COMMENT 'f',
    `customer_category1` string COMMENT 'gd',
    `customer_category2` string COMMENT 'f',
    `customer_category3` string COMMENT 'h',
    `customer_category4` string COMMENT 'g',
    `customer_category5` string COMMENT 'ff',
    `customer_category6` string COMMENT 'fdg',
    `customer_category7` string COMMENT 'g',
    `customer_category8` string COMMENT 'h',
    `customer_category9` string COMMENT 'g',
    `customer_category10` string COMMENT 'f',
    `consolidation_dock_loc_area` string COMMENT 'h',
    `consolidation_dock_loc_pos` string COMMENT 'g',
    `process_type` string COMMENT 'f',
    `creation_process_stamp` string COMMENT 'h',
    `creation_date_time_stamp` string COMMENT 'fg',
    `immediate_needs_note` string COMMENT 'gf',
    `allocate_complete` string COMMENT 'h',
    `alternate_email_address` string COMMENT 'g',
    `stop_sequence` double COMMENT 'f',
    `internal_carrier_num` double COMMENT 'g',
    `store_distribution` string COMMENT 'g',
    `total_lbr_estimate` double COMMENT 'g',
    `weight_entered` double COMMENT 'g',
    `volume_entered` double COMMENT 'h',
    `value_entered` double COMMENT 'fh',
    `locked` string COMMENT 'g',
    `shipper_code` string COMMENT 'f',
    `in_deletion` string COMMENT 'g',
    `inventory_load_confirmed` string COMMENT 'h',
    `last_status_uploaded` double COMMENT 'f',
    `user_def9` double COMMENT 'gh',
    `user_def10` double COMMENT 'f',
    `user_def11` string COMMENT 'g',
    `user_def12` string COMMENT 'h',
    `user_def13` string COMMENT 'f',
    `user_def14` string COMMENT 'g',
    `user_def15` string COMMENT 'gf',
    `user_def16` string COMMENT 'g',
    `user_def17_date_time` string COMMENT 'hf',
    `user_def18_date_time` string COMMENT 'g',
    `user_def19_date_time` string COMMENT 'd',
    `user_def20_date_time` string COMMENT 'd',
    `src_inc_day` string comment '数据来源的inc_day'
    ) COMMENT 'shipment - header' 
    PARTITIONED BY (`inc_day` string COMMENT '增量日期') 
    STORED AS Parquet 
    LOCATION 'hdfs://dsc/hive/warehouse/dsc/ods/
"""
 
+ i + "/shipment_header_df'"
for i in names]

sh2 = [i.replace('\n', '') for i in sh1]
[spark.sql(i) for i in sh2]


sd1 = ['CREATE EXTERNAL TABLE IF NOT EXISTS ' + i + 
    """.shipment_detail_df (
    `internal_shipment_line_num` double COMMENT 'h',
    `internal_shipment_num` double COMMENT 'g',
    `allocation_rule` string COMMENT 'g',
    `status_flow_name` string COMMENT 'h',
    `erp_order` string COMMENT 'f',
    `status1` double COMMENT 'gf',
    `erp_order_line_num` double COMMENT 'h',
    `warehouse` string COMMENT 'g',
    `internal_order_num` double COMMENT 'g',
    `status2` double COMMENT 'f',
    `ship_to` string COMMENT 'g',
    `status3` double COMMENT 'h',
    `launch_num` double COMMENT 'h',
    `status4` double COMMENT 'jhd',
    `status_failed` string COMMENT 'g',
    `status5` double COMMENT 'f',
    `status6` double COMMENT 'h',
    `status7` double COMMENT 'g',
    `order_type` string COMMENT 'h',
    `status8` double COMMENT 'f',
    `customer` string COMMENT 'j',
    `item` string COMMENT 'gh',
    `status9` double COMMENT 'h',
    `status10` double COMMENT 'j',
    `mark_for` string COMMENT 'gg',
    `item_desc` string COMMENT 'h',
    `customer_item` string COMMENT 'hg',
    `carrier` string COMMENT 'h',
    `carrier_service` string COMMENT 'g',
    `freight_terms` string COMMENT 'h',
    `liability_terms` string COMMENT 'hg',
    `order_date` string COMMENT 'g',
    `shipment_id` string COMMENT 'gh',
    `company` string COMMENT 'h',
    `interfaced_date` string COMMENT 'g',
    `requested_qty` double COMMENT 'h',
    `quantity_um` string COMMENT 'g',
    `pick_loc` string COMMENT 'f',
    `carrier_type` string COMMENT 'h',
    `pick_zone` string COMMENT 'j',
    `secondary_pick_loc` string COMMENT 'f',
    `weight_um` string COMMENT 'g',
    `secondary_pick_zone` string COMMENT 'f',
    `planned_ship_date` string COMMENT 'g',
    `item_length` double COMMENT 'j',
    `item_width` double COMMENT 'f',
    `requested_delivery_date` string COMMENT 'h',
    `requested_delivery_type` string COMMENT 'g',
    `item_height` double COMMENT 'j',
    `item_dimension_um` string COMMENT 'h',
    `item_department` string COMMENT 'hg',
    `item_list_price` double COMMENT 'h',
    `item_net_price` double COMMENT 'j',
    `item_color` string COMMENT 'h',
    `item_style` string COMMENT 'g',
    `item_size` string COMMENT 'gh',
    `original_item_ordered` string COMMENT 'h',
    `pick_list_id` string COMMENT 'gh',
    `customer_po` string COMMENT 'g',
    `invoice` string COMMENT 'h',
    `item_volume` double COMMENT 'gh',
    `packing_category` string COMMENT 'h',
    `total_volume` double COMMENT 'g',
    `hazardous_code` string COMMENT 'f',
    `item_division` string COMMENT 'g',
    `nmfc_code` string COMMENT 'gs',
    `total_weight` double COMMENT 'h',
    `catalog_id` string COMMENT 'g',
    `manufacture_id` string COMMENT 'd',
    `total_qty` double COMMENT 'g',
    `value` double COMMENT 'h',
    `merchandise_code` string COMMENT 'hg',
    `value_add_label_code` string COMMENT 'g',
    `volume_um` string COMMENT 'j',
    `user_def1` string COMMENT 'h',
    `user_def2` string COMMENT 'df',
    `user_def3` string COMMENT 's',
    `manually_entered` string COMMENT 'g',
    `user_def4` string COMMENT 'h',
    `user_def5` string COMMENT 'j',
    `user_def6` string COMMENT 'd',
    `user_def7` double COMMENT 'g',
    `user_def8` double COMMENT 'f',
    `user_stamp` string COMMENT 'd',
    `process_stamp` string COMMENT 'g',
    `quantity_at_sts1` double COMMENT 'h',
    `lot_controlled` string COMMENT 'd',
    `date_time_stamp` string COMMENT 'f',
    `serial_num_reqd` string COMMENT 'd',
    `quantity_at_sts2` double COMMENT 'h',
    `catch_weight_reqd` string COMMENT 'f',
    `quantity_at_sts3` double COMMENT 'df',
    `quantity_at_sts4` double COMMENT 'f',
    `quantity_at_sts5` double COMMENT 'df',
    `quantity_at_sts6` double COMMENT 'h',
    `quantity_at_sts7` double COMMENT 'd',
    `quantity_at_sts8` double COMMENT 'f',
    `quantity_at_sts9` double COMMENT 'h',
    `priority` double COMMENT 'f',
    `item_weight` double COMMENT 'a',
    `quantity_at_sts10` double COMMENT 'd',
    `lot` string COMMENT 'f',
    `item_class` string COMMENT 'g',
    `mark_for_name` string COMMENT 'd',
    `mark_for_address1` string COMMENT 'd',
    `mark_for_address2` string COMMENT 'f',
    `mark_for_address3` string COMMENT 'df',
    `mark_for_attention_to` string COMMENT 's',
    `mark_for_city` string COMMENT 'fg',
    `mark_for_state` string COMMENT 'h',
    `mark_for_country` string COMMENT 'g',
    `mark_for_postal_code` string COMMENT 'gf',
    `mark_for_phone_num` string COMMENT 'd',
    `mark_for_fax_num` string COMMENT 'fd',
    `mark_for_email_address` string COMMENT 'h',
    `harmonized_code` string COMMENT 'g',
    `harmonized_desc` string COMMENT 'f',
    `export_desc` string COMMENT 'f',
    `preference_crit` string COMMENT 'd',
    `producer` string COMMENT 'g',
    `net_cost` string COMMENT 'y',
    `country_of_origin` string COMMENT 'u',
    `packing_class` string COMMENT 'y',
    `interface_record_id` string COMMENT 'u',
    `item_category1` string COMMENT 'y',
    `item_category2` string COMMENT 'u',
    `item_category3` string COMMENT 'u',
    `item_category4` string COMMENT 'yu',
    `item_category5` string COMMENT 'u',
    `item_category6` string COMMENT 'y',
    `item_category7` string COMMENT 'u',
    `item_category8` string COMMENT 'y',
    `item_category9` string COMMENT 'u',
    `item_category10` string COMMENT 'uy',
    `cont_creation_full_qty` double COMMENT 'y',
    `cont_creation_full_qty_um` string COMMENT 'u',
    `cont_creation_innerpack_qty` double COMMENT 'y',
    `allocate_full_loc_qty` string COMMENT 'u',
    `allow_pct_alloc` string COMMENT 'y',
    `minimum_alloc_pct` double COMMENT 'u',
    `maximum_alloc_pct` double COMMENT 'i',
    `immediate_needs_note` string COMMENT 'y',
    `treat_as_loose` string COMMENT 'u',
    `previous_wave_num` double COMMENT 'u',
    `bom_action` string COMMENT '5',
    `internal_work_order_num` double COMMENT 'y',
    `related_internal_line_num` double COMMENT 'u',
    `quantity_needed_per_item` double COMMENT 'iy',
    `store_distribution` string COMMENT 'u',
    `immediate_needs_eligible` string COMMENT 't',
    `immediate_needs_loc_rule` string COMMENT 'ty',
    `logistics_unit` string COMMENT 'y',
    `parent_logistics_unit` string COMMENT 't',
    `loc_inv_attributes_id` double COMMENT 'y',
    `allocation_rejected_qty` double COMMENT 'y',
    `eccn` string COMMENT 't',
    `validated_license` string COMMENT 'ty',
    `license_exp_date` string COMMENT 'y',
    `src_inc_day` string comment '数据来源的inc_day'
    ) COMMENT 'shipment - 发货' 
    PARTITIONED BY (`inc_day` string COMMENT '增量日期') 
    STORED AS Parquet 
    LOCATION 'hdfs://dsc/hive/warehouse/dsc/ods/
"""
+ i + "/shipment_detail_df'"
for i in names]


sd2 = [i.replace('\n', '') for i in sd1]
[spark.sql(i) for i in sd2]



"""
====================================
====================================
   `purchase_order_line_number` double COMMENT 'HF',
    `src_inc_day` string comment '数据来源的inc_day'
    ) COMMENT 'receipt-detail' 
    PARTITIONED BY (`inc_day` string COMMENT '增量日期') 
    STORED AS Parquet 
    LOCATION 'hdfs://dsc/hive/warehouse/dsc/ods/
 
+ i + "/receipt_detail_df'"
for i in names]

"""

# names = [
#  'ods_cn_bose'
# ,'ods_cn_apple_sz'
# ,'ods_cn_apple_sh'
# ,'ods_cn_costacoffee'
# ,'ods_cn_diadora'
# ,'ods_cn_ferrero'
# ,'ods_cn_fuji'
# ,'ods_cn_hd'
# ,'ods_cn_hp_ljb'
# ,'ods_cn_hpi'
# ,'ods_cn_hualiancosta'
# ,'ods_cn_jiq'
# ,'ods_cn_kone'
# ,'ods_cn_michelin'
# ,'ods_cn_razer'
# ,'ods_cn_squibb'
# ,'ods_cn_vzug'
# ,'ods_cn_zebra'
# ,'ods_dbo'
# ,'ods_hk_abbott'
# ,'ods_hk_revlon'
# ,'ods_hk_fredperry'
# ] 

# dp1 = ['drop table if exists ' + i + '.receipt_header_df' for i in names]

# dp2 = [i.replace('\n', '') for i in dp1]

# [spark.sql(i) for i in dp2]


# sql_hive = "CREATE TABLE IF NOT EXISTS ods_dbo.receipt_header_df AS SELECT *,\
#       cast(null as string) as src_inc_day FROM ods_cn_bose.receipt_header where inc_day = '20210826' "; print(sql_hive)

# HiveContext(sql_hive)



# df = spark.sql("""select * from ods_cn_bose.receipt_header where inc_day = '20210826' limit 100""")
# df.show()
# df.createOrReplaceTempView('df_View') 
# for i in names:
#     seq = i + '.receipt_header_df'
#     df.write.mode("overwrite").saveAsTable(seq)
 
# rdd = spark.sparkContext.parallelize(data)
# rdd.toDF(columns) 

# my_DF.createOrReplaceTempView("my_temp_table");
# spark.sql("drop table if exists my_table");
# spark.sql("create table my_table as select * from my_temp_table");

# schema  = StructType([ 
#     StructField("internal_receipt_num",DoubleType(),True),
#     StructField("warehouse",StringType(),True), 
#     StructField("company",StringType(),True),
#     StructField("receipt_id",StringType(),True),
#     StructField("receipt_id_type",StringType(),True),
#     StructField("receipt_type",StringType(),True),
#     StructField("receipt_date",StringType(),True),
#     StructField("close_date",StringType(),True),
#     StructField("source_id",StringType(),True),
#     StructField("source_name",StringType(),True),
#     StructField("source_address1",StringType(),True),
#     StructField("source_address2",StringType(),True),
#     StructField("source_address3",StringType(),True),
#     StructField("source_city",StringType(),True),
#     StructField("source_state",StringType(),True),
#     StructField("source_postal_code",StringType(),True),
#     StructField("source_country",StringType(),True),
#     StructField("source_attention_to",StringType(),True),
#     StructField("source_phone_num",StringType(),True),
#     StructField("source_fax_num",StringType(),True),
#     StructField("source_email_address",StringType(),True),
#     StructField("priority",LongType(),True),
#     StructField("carrier",StringType(),True),
#     StructField("carrier_service",StringType(),True),
#     StructField("erp_order_num",StringType(),True),
#     StructField("erp_order_type",StringType(),True),
#     StructField("bol_num_alpha",StringType(),True),
#     StructField("license_plate_id",StringType(),True),
#     StructField("packing_list_id",StringType(),True),
#     StructField("pro_num_alpha",StringType(),True),
#     StructField("trailer_id",StringType(),True),
#     StructField("seal_id",StringType(),True),
#     StructField("total_containers",DoubleType(),True),
#     StructField("total_lines",DoubleType(),True),
#     StructField("total_qty",DoubleType(),True),
#     StructField("quantity_um",StringType(),True),
#     StructField("total_weight",DoubleType(),True),
#     StructField("weight_um",StringType(),True),
#     StructField("total_volume",DoubleType(),True),
#     StructField("volume_um",StringType(),True),
#     StructField("total_value",DoubleType(),True),
#     StructField("leading_sts",StringType(),True),
#     StructField("leading_sts_date",LongType(),True),
#     StructField("leading_sts_failed",StringType(),True),
#     StructField("trailing_sts",LongType(),True),
#     StructField("trailing_sts_date",StringType(),True),
#     StructField("trailing_sts_failed",StringType(),True),
#     StructField("user_def1",StringType(),True),
#     StructField("user_def2",StringType(),True),
#     StructField("user_def3",StringType(),True),
#     StructField("user_def4",StringType(),True),
#     StructField("user_def5",StringType(),True),
#     StructField("user_def6",StringType(),True),
#     StructField("user_def7",DoubleType(),True),
#     StructField("user_def8",DoubleType(),True),
#     StructField("user_stamp",StringType(),True),
#     StructField("process_stamp",StringType(),True),
#     StructField("date_time_stamp",StringType(),True),
#     StructField("manually_entered",StringType(),True),
#     StructField("ship_from",StringType(),True),
#     StructField("ship_from_address1",StringType(),True),
#     StructField("ship_from_address2",StringType(),True),
#     StructField("ship_from_address3",StringType(),True),
#     StructField("ship_from_city",StringType(),True),
#     StructField("ship_from_state",StringType(),True),
#     StructField("ship_from_country",StringType(),True),
#     StructField("ship_from_postal_code",StringType(),True),
#     StructField("ship_from_name",StringType(),True),
#     StructField("ship_from_attention_to",StringType(),True),
#     StructField("ship_from_email_address",StringType(),True),
#     StructField("ship_from_phone_num",StringType(),True),
#     StructField("ship_from_fax_num",StringType(),True),
#     StructField("scheduled_date_time",StringType(),True),
#     StructField("arrived_date_time",StringType(),True),
#     StructField("start_unitize_date_time",StringType(),True),
#     StructField("end_unitize_date_time",StringType(),True),
#     StructField("interface_record_id",StringType(),True),
#     StructField("creation_process_stamp",StringType(),True),
#     StructField("creation_date_time_stamp",StringType(),True),
#     StructField("trailer_yard_status_id",StringType(),True),
#     StructField("upload_interface_batch",StringType(),True),
#     StructField("in_pre_checkin_ctr_creation",StringType(),True),
#     StructField("src_inc_day",StringType(),True)
# ])

# print(np.arange(0,83).astype(str))

 

# df = spark.createDataFrame(data =list(np.arange(0,83).astype(str)), schema=schema)
# df.printSchema()
# df.show(truncate=False)
# df.createOrReplaceTempView('df_final_View') #source_2
# spark.sql('INSERT INTO ods_cn_apple_sz SELECT * FROM df_final_View')



# for i in names:
#     sql = """hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS """ + i +  """.receipt_header_df (
#     `internal_receipt_num` double COMMENT '4',
#     `warehouse` string COMMENT '4',
#     `company` string COMMENT '4',
#     `receipt_id` string COMMENT '4',
#     `receipt_id_type` string COMMENT '4',
#     `receipt_type` string COMMENT '4',
#     `receipt_date` string COMMENT '4',
#     `close_date` string COMMENT '4',
#     `source_id` string COMMENT '4',
#     `source_name` string COMMENT '4',
#     `source_address1` string COMMENT '4',
#     `source_address2` string COMMENT '4',
#     `source_address3` string COMMENT '4',
#     `source_city` string COMMENT '4',
#     `source_state` string COMMENT '4',
#     `source_postal_code` string COMMENT '4',
#     `source_country` string COMMENT '4',
#     `source_attention_to` string COMMENT '4',
#     `source_phone_num` string COMMENT '4',
#     `source_fax_num` string COMMENT '4',
#     `source_email_address` string COMMENT '4',
#     `priority` double COMMENT '4',
#     `carrier` string COMMENT '4',
#     `carrier_service` string COMMENT '4',
#     `erp_order_num` string COMMENT '4',
#     `erp_order_type` string COMMENT '4',
#     `bol_num_alpha` string COMMENT '4',
#     `license_plate_id` string COMMENT '4',
#     `packing_list_id` string COMMENT '4',
#     `pro_num_alpha` string COMMENT '4',
#     `trailer_id` string COMMENT '4',
#     `seal_id` string COMMENT '4',
#     `total_containers` double COMMENT '4',
#     `total_lines` double COMMENT '4',
#     `total_qty` double COMMENT '4',
#     `quantity_um` string COMMENT '4',
#     `total_weight` double COMMENT '4',
#     `weight_um` string COMMENT '4',
#     `total_volume` double COMMENT '4',
#     `volume_um` string COMMENT '4',
#     `total_value` double COMMENT '4',
#     `leading_sts` double COMMENT '4',
#     `leading_sts_date` string COMMENT '4',
#     `leading_sts_failed` string COMMENT '4',
#     `trailing_sts` double COMMENT '4',
#     `trailing_sts_date` string COMMENT '4',
#     `trailing_sts_failed` string COMMENT '4',
#     `user_def1` string COMMENT '4',
#     `user_def2` string COMMENT '4',
#     `user_def3` string COMMENT '4',
#     `user_def4` string COMMENT '4',
#     `user_def5` string COMMENT '4',
#     `user_def6` string COMMENT '4',
#     `user_def7` double COMMENT '4',
#     `user_def8` double COMMENT '4',
#     `user_stamp` string COMMENT '4',
#     `process_stamp` string COMMENT '4',
#     `date_time_stamp` string COMMENT '4',
#     `manually_entered` string COMMENT '4',
#     `ship_from` string COMMENT '4',
#     `ship_from_address1` string COMMENT '4',
#     `ship_from_address2` string COMMENT '4',
#     `ship_from_address3` string COMMENT '4',
#     `ship_from_city` string COMMENT '4',
#     `ship_from_state` string COMMENT '4',
#     `ship_from_country` string COMMENT '4',
#     `ship_from_postal_code` string COMMENT '4',
#     `ship_from_name` string COMMENT '4',
#     `ship_from_attention_to` string COMMENT '4',
#     `ship_from_email_address` string COMMENT '4',
#     `ship_from_phone_num` string COMMENT '4',
#     `ship_from_fax_num` string COMMENT '4',
#     `scheduled_date_time` string COMMENT '4',
#     `arrived_date_time` string COMMENT '4',
#     `start_unitize_date_time` string COMMENT '4',
#     `end_unitize_date_time` string COMMENT '4',
#     `interface_record_id` string COMMENT '4',
#     `creation_process_stamp` string COMMENT '4',
#     `creation_date_time_stamp` string COMMENT '4',
#     `trailer_yard_status_id` double COMMENT '4',
#     `upload_interface_batch` string COMMENT '4',
#     `in_pre_checkin_ctr_creation` string COMMENT '4',
#     `src_inc_day` string comment '数据来源的inc_day') 
#     COMMENT 'RECEIPT_HEADER' 
#     PARTITIONED BY (`inc_day` string COMMENT '增量日期') 
#     STORED AS PARQUET
#     LOCATION 'hdfs://dsc/hive/warehouse/dsc/DWD/dsc_dwd/""" + i + ".receipt_header_df'" + """ " """

#     sql = sql.replace("\n","")
#     # cmd = 'hive -e "' + sql + '"'
#     # cmd.replace("\\", '')
#     print(sql)
#     subprocess.check_output(sql)


 # call
# df.createOrReplaceTempView("test")
# df = spark.sql("""
# select 
# internal_receipt_num
# ,warehouse
# ,company
# ,receipt_id
# ,receipt_id_type
# ,receipt_type
# ,receipt_date
# ,close_date
# ,source_id
# ,source_name
# ,source_address1
# ,source_address2
# ,source_address3
# ,source_city
# ,source_state
# ,source_postal_code
# ,source_country
# ,source_attention_to
# ,source_phone_num
# ,source_fax_num
# ,source_email_address
# ,priority
# ,carrier
# ,carrier_service
# ,erp_order_num
# ,erp_order_type
# ,bol_num_alpha
# ,license_plate_id
# ,packing_list_id
# ,pro_num_alpha
# ,trailer_id
# ,seal_id
# ,total_containers
# ,total_lines
# ,total_qty
# ,quantity_um
# ,total_weight
# ,weight_um
# ,total_volume
# ,volume_um
# ,total_value
# ,leading_sts
# ,leading_sts_date
# ,leading_sts_failed
# ,trailing_sts
# ,trailing_sts_date
# ,trailing_sts_failed
# ,user_def1
# ,user_def2
# ,user_def3
# ,user_def4
# ,user_def5
# ,user_def6
# ,user_def7
# ,user_def8
# ,user_stamp
# ,process_stamp
# ,date_time_stamp
# ,manually_entered
# ,ship_from
# ,ship_from_address1
# ,ship_from_address2
# ,ship_from_address3
# ,ship_from_city
# ,ship_from_state
# ,ship_from_country
# ,ship_from_postal_code
# ,ship_from_name
# ,ship_from_attention_to
# ,ship_from_email_address
# ,ship_from_phone_num
# ,ship_from_fax_num
# ,scheduled_date_time
# ,arrived_date_time
# ,start_unitize_date_time
# ,end_unitize_date_time
# ,interface_record_id
# ,creation_process_stamp
# ,creation_date_time_stamp
# ,trailer_yard_status_id
# ,upload_interface_batch
# ,in_pre_checkin_ctr_creation
# ,src_inc_day
# """)
